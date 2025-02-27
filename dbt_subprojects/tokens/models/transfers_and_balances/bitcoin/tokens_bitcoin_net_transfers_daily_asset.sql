{{ config(
        schema = 'tokens_bitcoin'
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
        'bitcoin' as blockchain
        , block_date
        , wallet_address as address
        , 'sent' as transfer_direction
        , (sum(abs(amount_transfer_usd)) * -1) as transfer_amount_usd
        , count(*) transfer_count
    from  {{ source('transfers_bitcoin', 'satoshi') }}
    where
        1 = 1
        and type = 'input'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , wallet_address
        , 'sent'

    union all

    select
        'bitcoin' as blockchain
        , block_date
        , wallet_address as address
        , 'received' as transfer_direction
        , sum(abs(amount_transfer_usd)) as transfer_amount_usd
        , count(*) transfer_count
    from  {{ source('transfers_bitcoin', 'satoshi') }}
    where
        1 = 1
        and type = 'output'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , wallet_address
        , 'received'
), transfers_amount as (
    select
        t.blockchain
        , t.block_date
        , t.address
        , sum(case when t.transfer_direction = 'sent' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_sent
        , sum(case when t.transfer_direction = 'received' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_received
        , sum(transfer_count) as transfer_count
    from
        raw_transfers as t
    group by
        t.blockchain
        , t.block_date
        , t.address
), net_transfers as (
    select
        blockchain
        , block_date
        , address
        , sum(coalesce(transfer_amount_usd_sent, 0)) as transfer_amount_usd_sent
        , sum(coalesce(transfer_amount_usd_received, 0)) as transfer_amount_usd_received
        , sum(coalesce(transfer_amount_usd_received, 0)) + sum(coalesce(transfer_amount_usd_sent, 0)) as net_transfer_amount_usd
        , sum(transfer_count) transfer_count
    from
        transfers_amount
    group by
        blockchain
        , block_date
        , address
)
select
    blockchain
    , block_date
    , cast('0x0000000000000000000000000000000000' as varchar(42)) as contract_address
    , 'BTC' as symbol
    , sum(transfer_amount_usd_sent) as transfer_amount_usd_sent
    , sum(transfer_amount_usd_received) as transfer_amount_usd_received
    , sum(abs(transfer_amount_usd_sent)) + sum(abs(transfer_amount_usd_received)) as transfer_amount_usd
    , sum(net_transfer_amount_usd) as net_transfer_amount_usd
    , sum(transfer_count) transfer_count
from
    net_transfers
where
    net_transfer_amount_usd > 0
group by
    blockchain
    , block_date

