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
    from
        {{ ref('tokens_solana_transfers') }}
    where
        1 = 1
        and action != 'wrap'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
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
    from
        {{ ref('tokens_solana_transfers') }}
    where
        1 = 1
        and action != 'wrap'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
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
    from
        raw_transfers as t
    group by
        t.blockchain
        , t.block_date
        , t.contract_address
        , t.symbol
        , t.address
), net_transfers as (
    select
        blockchain
        , block_date
        , contract_address
        , symbol
        , address
        , sum(coalesce(transfer_amount_usd_sent, 0)) as transfer_amount_usd_sent
        , sum(coalesce(transfer_amount_usd_received, 0)) as transfer_amount_usd_received
        , sum(coalesce(transfer_amount_usd_received, 0)) + sum(coalesce(transfer_amount_usd_sent, 0)) as net_transfer_amount_usd
        , sum(transfer_count) as transfer_count
    from
        transfers_amount
    group by
        blockchain
        , block_date
        , contract_address
        , symbol
        , address
)
select
    blockchain
    , block_date
    , contract_address
    , symbol
    , sum(net_transfer_amount_usd) as net_transfer_amount_usd
from
    net_transfers
where
    net_transfer_amount_usd > 0
group by
    blockchain
    , block_date
    , contract_address
    , symbol