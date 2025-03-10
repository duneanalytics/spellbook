{{ config(
        schema = 'tokens_solana'
        , alias = 'net_transfers_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with raw_transfers as (
    select
        'solana' as blockchain
        , block_date
        , from_owner as address
        , 'sent' as transfer_direction
        , (sum(amount_usd) * -1) as transfer_amount_usd
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
        , from_owner
        , 'sent'

    union all

    select
        'solana' as blockchain
        , block_date
        , to_owner as address
        , 'received' as transfer_direction
        , sum(amount_usd) as transfer_amount_usd
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
        , to_owner
        , 'received'
), transfers_amount as (
    select
        t.blockchain
        , t.block_date
        , t.address
        , sum(case when t.transfer_direction = 'sent' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_sent
        , sum(case when t.transfer_direction = 'received' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_received
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
    , sum(net_transfer_amount_usd) as net_transfer_amount_usd
from
    net_transfers
where
    net_transfer_amount_usd > 0
group by
    blockchain
    , block_date
