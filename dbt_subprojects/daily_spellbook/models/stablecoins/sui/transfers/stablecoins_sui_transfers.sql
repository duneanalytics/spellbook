{{ config(
    schema = 'stablecoins_sui'
    , alias = 'transfers'
    , partition_by = ['block_date']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'unique_key']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , tags = ['sui', 'stablecoin', 'transfers']
) }}

-- ============================================================
-- SUI Stablecoin Transfers (USDC)
--
-- Enhances #9308 with event-based Circle CCTP mint/burn tracking.
--
-- Key finding: burns MUST use events because Deleted objects
-- lose their coin_type on Sui. The #9308 object-based approach
-- silently produces $0 burn volume.
--
-- Mint/burn amounts are in the treasury event JSON payload:
--   Mint: {"amount": "826829", "recipient": "0x39bb..."}
--   Burn: {"amount": "13846570", "mint_cap": "0xee9b..."}
--
-- Direct sends use the same object-based approach as #9308.
-- ============================================================

{% set usdc_coin_type = '0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC' %}
{% set usdc_decimals = 6 %}
{% set usdc_start_date = '2024-09-18' %}

-- ============================================================
-- 1. DIRECT SENDS
--    Created coin objects where tx sender != object owner.
--    Same approach as #9308 credit-side. One row per recipient.
-- ============================================================
with direct_sends as (
    select
        {{ dbt_utils.generate_surrogate_key(['o.previous_transaction', 'o.object_id', "cast(o.version as varchar)"]) }} as unique_key
        , 'sui'                                                     as blockchain
        , cast(date_trunc('month', o.date) as date)                 as block_month
        , o.date                                                    as block_date
        , from_unixtime(o.timestamp_ms / 1000)                      as block_time
        , o.checkpoint                                              as block_number
        , o.previous_transaction                                    as tx_hash
        , cast(null as bigint)                                      as evt_index
        , 'sui_coin'                                                as token_standard
        , '{{ usdc_coin_type }}'                                    as token_address
        , 'USDC'                                                    as token_symbol
        , 'usd'                                                     as currency
        , try_cast(o.coin_balance as bigint)                        as amount_raw
        , try_cast(o.coin_balance as double) / power(10, {{ usdc_decimals }}) as amount
        , cast(1.0 as double)                                       as price_usd
        , try_cast(o.coin_balance as double) / power(10, {{ usdc_decimals }}) as amount_usd
        , t.sender                                                  as "from"
        , o.owner_address                                           as "to"
        , 'direct_send'                                             as transfer_type
        , false                                                     as is_supply_event
        , cast(null as varchar)                                     as supply_event_type
        , o.object_id                                               as object_id
        , t.sender                                                  as tx_sender
    from {{ source('sui', 'objects') }} o
    inner join {{ source('sui', 'transactions') }} t
        on o.previous_transaction = t.transaction_digest
        and t.date >= date '{{ usdc_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('t.date') }}
        {% endif %}
    where o.object_status = 'Created'
        and o.coin_type = '{{ usdc_coin_type }}'
        and try_cast(o.coin_balance as bigint) > 0
        and t.sender != o.owner_address
        and o.date >= date '{{ usdc_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('o.date') }}
        {% endif %}
)

-- ============================================================
-- 2. MINTS (Circle CCTP)
--    Amount and recipient from treasury::Mint event JSON.
--    Verified exact match with object approach (188:188, $0 diff).
--
--    Sample query to find these events:
--      SELECT transaction_digest, event_json
--      FROM sui.events
--      WHERE event_type LIKE '%treasury::Mint<...USDC...>%'
--      AND date = DATE '2026-04-01'
-- ============================================================
, mints as (
    select
        {{ dbt_utils.generate_surrogate_key(['e.transaction_digest', "cast(e.event_index as varchar)"]) }} as unique_key
        , 'sui'                                                     as blockchain
        , cast(date_trunc('month', e.date) as date)                 as block_month
        , e.date                                                    as block_date
        , from_unixtime(e.timestamp_ms / 1000)                      as block_time
        , e.checkpoint                                              as block_number
        , e.transaction_digest                                      as tx_hash
        , e.event_index                                             as evt_index
        , 'sui_coin'                                                as token_standard
        , '{{ usdc_coin_type }}'                                    as token_address
        , 'USDC'                                                    as token_symbol
        , 'usd'                                                     as currency
        , cast(json_extract_scalar(e.event_json, '$.amount') as bigint) as amount_raw
        , cast(json_extract_scalar(e.event_json, '$.amount') as double) / power(10, {{ usdc_decimals }}) as amount
        , cast(1.0 as double)                                       as price_usd
        , cast(json_extract_scalar(e.event_json, '$.amount') as double) / power(10, {{ usdc_decimals }}) as amount_usd
        , e.sender                                                  as "from"
        , from_hex(substr(json_extract_scalar(e.event_json, '$.recipient'), 3)) as "to"
        , 'mint'                                                    as transfer_type
        , true                                                      as is_supply_event
        , 'mint'                                                    as supply_event_type
        , cast(null as varbinary)                                   as object_id
        , e.sender                                                  as tx_sender
    from {{ source('sui', 'events') }} e
    where e.event_type like '%treasury::Mint<{{ usdc_coin_type }}>%'
        and e.date >= date '{{ usdc_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('e.date') }}
        {% endif %}
)

-- ============================================================
-- 3. BURNS (Circle CCTP)
--    Amount from treasury::Burn event JSON.
--    CRITICAL: Deleted objects lose coin_type on Sui — this is
--    the ONLY working approach for burn amounts.
--
--    Sample query to find these events:
--      SELECT transaction_digest, event_json
--      FROM sui.events
--      WHERE event_type LIKE '%treasury::Burn<...USDC...>%'
--      AND date = DATE '2026-04-01'
-- ============================================================
, burns as (
    select
        {{ dbt_utils.generate_surrogate_key(['e.transaction_digest', "cast(e.event_index as varchar)"]) }} as unique_key
        , 'sui'                                                     as blockchain
        , cast(date_trunc('month', e.date) as date)                 as block_month
        , e.date                                                    as block_date
        , from_unixtime(e.timestamp_ms / 1000)                      as block_time
        , e.checkpoint                                              as block_number
        , e.transaction_digest                                      as tx_hash
        , e.event_index                                             as evt_index
        , 'sui_coin'                                                as token_standard
        , '{{ usdc_coin_type }}'                                    as token_address
        , 'USDC'                                                    as token_symbol
        , 'usd'                                                     as currency
        , cast(json_extract_scalar(e.event_json, '$.amount') as bigint) as amount_raw
        , cast(json_extract_scalar(e.event_json, '$.amount') as double) / power(10, {{ usdc_decimals }}) as amount
        , cast(1.0 as double)                                       as price_usd
        , cast(json_extract_scalar(e.event_json, '$.amount') as double) / power(10, {{ usdc_decimals }}) as amount_usd
        , e.sender                                                  as "from"
        , from_hex(substr(split_part(e.event_type, '::', 1), 3))    as "to"
        , 'burn'                                                    as transfer_type
        , true                                                      as is_supply_event
        , 'burn'                                                    as supply_event_type
        , cast(null as varbinary)                                   as object_id
        , e.sender                                                  as tx_sender
    from {{ source('sui', 'events') }} e
    where e.event_type like '%treasury::Burn<{{ usdc_coin_type }}>%'
        and e.date >= date '{{ usdc_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('e.date') }}
        {% endif %}
)

select * from direct_sends
union all
select * from mints
union all
select * from burns
