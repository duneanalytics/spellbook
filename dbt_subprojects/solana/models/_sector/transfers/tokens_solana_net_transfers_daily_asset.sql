{{ config(
        schema = 'tokens_solana'
        , alias = 'net_transfers_daily_asset'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'contract_address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
)
}}

with raw_transfers as (
    select
        'solana' as blockchain
        , block_date
        , token_mint_address as contract_address
        , symbol
        , from_owner as address
        , 'sent' as transfer_direction
        , (sum(amount_usd) * -1) as transfer_amount_usd
        , count(*) transfer_count
        , max(block_time) as max_block_time  -- Track most recent block_time per symbol
    from
        {{ source('tokens_solana','transfers') }}
    where
        1 = 1
        and action != 'wrap'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
        {% if target.name == 'ci' %}
        -- bound the full-refresh scan in CI so the build completes within the 90-min cap; prod is unaffected
        and block_date >= current_date - interval '3' day
        {% endif %}
    group by
        'solana'
        , block_date
        , token_mint_address
        , symbol
        , from_owner
        , 'sent'

    union all

    select
        'solana' as blockchain
        , block_date
        , token_mint_address as contract_address
        , symbol
        , to_owner as address
        , 'received' as transfer_direction
        , sum(amount_usd) as transfer_amount_usd
        , count(*) transfer_count
        , max(block_time) as max_block_time  -- Track most recent block_time per symbol
    from
        {{ source('tokens_solana','transfers') }}
    where
        1 = 1
        and action != 'wrap'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
        {% if target.name == 'ci' %}
        -- bound the full-refresh scan in CI so the build completes within the 90-min cap; prod is unaffected
        and block_date >= current_date - interval '3' day
        {% endif %}
    group by
        'solana'
        , block_date
        , token_mint_address
        , symbol
        , to_owner
        , 'received'
), transfers_amount as (
    select
        t.blockchain
        , t.block_date
        , t.contract_address
        , t.symbol
        , t.address
        , sum(case when t.transfer_direction = 'sent' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_sent
        , sum(case when t.transfer_direction = 'received' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_received
        , sum(t.transfer_count) as transfer_count
        , max(t.max_block_time) as max_block_time  -- Preserve max block_time
    from
        raw_transfers as t
    group by
        t.blockchain
        , t.block_date
        , t.contract_address
        , t.symbol
        , t.address
)
-- transfers_amount is already at the (contract_address, address) grain, so the
-- per-address net (received + sent) can be aggregated straight to the asset level
-- without a redundant intermediate group-by over the same keys.
select
    blockchain
    , block_date
    , contract_address
    , max_by(symbol, max_block_time) as symbol
    , sum(coalesce(transfer_amount_usd_received, 0) + coalesce(transfer_amount_usd_sent, 0)) as net_transfer_amount_usd
from
    transfers_amount
where
    (coalesce(transfer_amount_usd_received, 0) + coalesce(transfer_amount_usd_sent, 0)) > 0
group by
    blockchain
    , block_date
    , contract_address