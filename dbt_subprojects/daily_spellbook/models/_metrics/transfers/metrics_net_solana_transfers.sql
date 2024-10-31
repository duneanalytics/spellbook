{{ config(
        schema = 'metrics'
        , alias = 'net_solana_transfers'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'tx_hash']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with raw_transfers as (
    /*
    - get the transfer amount sent per address in a transaction hash, set the amount sent as a negative value
    - union the transfer amount received per address in a transaction hash, keep the amount received as a positve value
    */
    select
        blockchain
        , block_date
        , tx_id as tx_hash
        , from_base58(from_owner) as address
        , 'sent' as transfer_direction
        , (sum(amount_usd) * -1) as transfer_amount_usd
    from
        {{ source('tokens_solana', 'transfers') }}
    where
        1 = 1
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , tx_id
        , from_base58(from_owner)
        , 'sent'

    union all

    select
        blockchain
        , block_date
        , tx_id as tx_hash
        , from_base58(to_owner) as address
        , 'received' as transfer_direction
        , sum(amount_usd) as transfer_amount_usd
    from
        {{ source('tokens_solana', 'transfers') }}
    where
        1 = 1
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , tx_id
        , from_base58(to_owner)
        , 'received'
), transfers_amount as (
    /*
    - create one column for transfer amount received, one column for transfer amount sent
    */
    select
        t.blockchain
        , t.block_date
        , t.tx_hash
        , t.address
        , sum(case when t.transfer_direction = 'sent' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_sent
        , sum(case when t.transfer_direction = 'received' then t.transfer_amount_usd else 0 end) as transfer_amount_usd_received
    from
        raw_transfers as t
    group by
        t.blockchain
        , t.block_date
        , t.tx_hash
        , t.address
), net_transfers as (
    /*
    - add amount received and amount sent (since transfer amount sent is set to a negative number, this calculates the net transfer amount received)
    - for any given address in a single transaction
    */
    select
        blockchain
        , block_date
        , tx_hash
        , address
        , sum(coalesce(transfer_amount_usd_sent, 0)) as transfer_amount_usd_sent
        , sum(coalesce(transfer_amount_usd_received, 0)) as transfer_amount_usd_received
        , sum(coalesce(transfer_amount_usd_received, 0)) + sum(coalesce(transfer_amount_usd_sent, 0)) as net_transfer_amount_usd
    from
        transfers_amount
    group by
        blockchain
        , block_date
        , tx_hash
        , address
)
select 
    blockchain
    , block_date
    , tx_hash
    , sum(transfer_amount_usd_sent) as transfer_amount_usd_sent
    , sum(transfer_amount_usd_received) as transfer_amount_usd_received
    , sum(net_transfer_amount_usd) as net_transfer_amount_usd
from
    net_transfers
where
    net_transfer_amount_usd > 0
group by
    blockchain
    , block_date
    , tx_hash