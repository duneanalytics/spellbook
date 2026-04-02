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
-- Enhances #9308 with two changes:
--   1. Event-based Circle CCTP mint/burn tracking
--      (fixes burn bug: Deleted objects lose coin_type on Sui)
--   2. Credit-side ergonomic output (1 row per transfer)
--
-- Object pipeline (day_rows → anchors → calc → enriched) is
-- kept from #9308 for direct sends + ownership transfers.
-- Mint/burn amounts come from treasury event JSON payloads.
-- ============================================================

{% set usdc_coin_type = '0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC' %}
{% set usdc_decimals = 6 %}
{% set usdc_start_date = '2024-09-18' %}

-- ============================================================
-- OBJECT PIPELINE (from #9308)
-- Tracks coin object balance deltas and ownership changes
-- ============================================================
with day_rows as (
    select
        o.object_id
        , o.version
        , o.previous_transaction                                    as tx_digest
        , o.timestamp_ms
        , o.date                                                    as block_date
        , cast(date_trunc('month', o.date) as date)                 as block_month
        , o.checkpoint
        , o.owner_type
        , o.owner_address                                           as receiver
        , o.coin_type
        , o.object_status
        , try_cast(o.coin_balance as bigint)                        as coin_balance
    from {{ source('sui', 'objects') }} o
    where o.object_status in ('Created', 'Mutated')
        and o.coin_type = '{{ usdc_coin_type }}'
        and o.date >= date '{{ usdc_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('o.date') }}
        {% endif %}
)

, anchors as (
    select
        p.object_id
        , max(p.version)                                            as version
        , cast(null as varchar)                                     as tx_digest
        , max_by(p.timestamp_ms, p.version)                         as timestamp_ms
        , cast(date '{{ usdc_start_date }}' as date)                as block_date
        , cast(date_trunc('month', date '{{ usdc_start_date }}') as date) as block_month
        , max_by(p.checkpoint, p.version)                           as checkpoint
        , max_by(p.owner_type, p.version)                           as owner_type
        , max_by(p.owner_address, p.version)                        as receiver
        , p.coin_type
        , cast('ANCHOR' as varchar)                                 as object_status
        , max_by(try_cast(p.coin_balance as bigint), p.version)     as coin_balance
    from {{ source('sui', 'objects') }} p
    where p.object_status in ('Created', 'Mutated')
        and p.coin_type = '{{ usdc_coin_type }}'
        and p.date < date '{{ usdc_start_date }}'
        and p.object_id in (
            select distinct d.object_id from day_rows d
        )
    group by p.object_id, p.coin_type
)

, unioned as (
    select * from anchors
    union all
    select * from day_rows
)

, calc as (
    select
        u.object_id
        , u.version
        , u.tx_digest
        , u.timestamp_ms
        , u.block_date
        , u.block_month
        , u.checkpoint
        , coalesce(u.owner_type,  lag(u.owner_type)  over w)        as owner_type
        , coalesce(u.receiver,    lag(u.receiver)     over w)        as receiver
        , coalesce(u.coin_type,   lag(u.coin_type)    over w)        as coin_type
        , u.object_status
        , u.coin_balance
        , lag(u.receiver)     over w                                 as prev_owner
        , lag(u.coin_balance) over w                                 as prev_balance
    from unioned u
    window w as (partition by u.object_id order by u.version)
)

, tx_senders as (
    select distinct
        t.transaction_digest                                         as tx_digest
        , t.sender                                                   as tx_sender
    from {{ source('sui', 'transactions') }} t
    inner join (
        select distinct c.tx_digest
        from calc c
        where c.tx_digest is not null
    ) d
        on t.transaction_digest = d.tx_digest
    where t.date >= date '{{ usdc_start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('t.date') }}
        {% endif %}
)

, enriched as (
    select
        c.*
        , s.tx_sender
        , c.coin_balance - coalesce(c.prev_balance, 0)              as balance_delta
        , case
            when c.object_status in ('Created', 'Deleted') then false
            when c.object_status = 'Mutated'
                 and c.prev_owner is not null
                 and c.prev_owner != c.receiver                     then true
            else false
        end                                                          as has_ownership_change
    from calc c
    left join tx_senders s
        on c.tx_digest = s.tx_digest
    where c.object_status != 'ANCHOR'
)

, filtered as (
    select * from enriched
    where balance_delta != 0
       or has_ownership_change
)

-- ============================================================
-- TRANSFER OUTPUT: credit-side only, 1 row per transfer
-- ============================================================

-- Direct sends: Created objects where tx_sender != receiver
, direct_sends as (
    select
        {{ dbt_utils.generate_surrogate_key(['f.tx_digest', 'f.object_id', "cast(f.version as varchar)"]) }} as unique_key
        , 'sui'                                                     as blockchain
        , f.block_month
        , f.block_date
        , from_unixtime(f.timestamp_ms / 1000)                      as block_time
        , f.checkpoint                                              as block_number
        , f.tx_digest                                               as tx_hash
        , cast(null as bigint)                                      as evt_index
        , 'sui_coin'                                                as token_standard
        , '{{ usdc_coin_type }}'                                    as token_address
        , 'USDC'                                                    as token_symbol
        , 'usd'                                                     as currency
        , f.coin_balance                                            as amount_raw
        , cast(f.coin_balance as double) / power(10, {{ usdc_decimals }}) as amount
        , cast(1.0 as double)                                       as price_usd
        , cast(f.coin_balance as double) / power(10, {{ usdc_decimals }}) as amount_usd
        , coalesce(f.tx_sender, f.prev_owner)                       as "from"
        , f.receiver                                                as "to"
        , 'direct_send'                                             as transfer_type
        , false                                                     as is_supply_event
        , cast(null as varchar)                                     as supply_event_type
        , f.object_id
        , f.tx_sender
    from filtered f
    where f.object_status = 'Created'
        and f.tx_sender is not null
        and f.tx_sender != f.receiver
        and f.coin_balance > 0
)

-- Ownership transfers: Mutated objects where owner changed
, ownership_transfers as (
    select
        {{ dbt_utils.generate_surrogate_key(['f.tx_digest', 'f.object_id', "cast(f.version as varchar)"]) }} as unique_key
        , 'sui'                                                     as blockchain
        , f.block_month
        , f.block_date
        , from_unixtime(f.timestamp_ms / 1000)                      as block_time
        , f.checkpoint                                              as block_number
        , f.tx_digest                                               as tx_hash
        , cast(null as bigint)                                      as evt_index
        , 'sui_coin'                                                as token_standard
        , '{{ usdc_coin_type }}'                                    as token_address
        , 'USDC'                                                    as token_symbol
        , 'usd'                                                     as currency
        , f.coin_balance                                            as amount_raw
        , cast(f.coin_balance as double) / power(10, {{ usdc_decimals }}) as amount
        , cast(1.0 as double)                                       as price_usd
        , cast(f.coin_balance as double) / power(10, {{ usdc_decimals }}) as amount_usd
        , f.prev_owner                                              as "from"
        , f.receiver                                                as "to"
        , 'transfer_with_balance_change'                            as transfer_type
        , false                                                     as is_supply_event
        , cast(null as varchar)                                     as supply_event_type
        , f.object_id
        , f.tx_sender
    from filtered f
    where f.has_ownership_change
        and f.balance_delta > 0
        and f.coin_balance > 0
)

-- ============================================================
-- EVENT-BASED MINTS AND BURNS (Circle CCTP)
-- Fixes #9308 burn bug: Deleted objects lose coin_type on Sui
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

-- ============================================================
-- UNION ALL
-- ============================================================
select * from direct_sends
union all
select * from ownership_transfers
union all
select * from mints
union all
select * from burns
