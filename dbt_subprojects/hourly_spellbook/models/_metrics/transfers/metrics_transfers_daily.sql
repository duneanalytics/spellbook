{{ config(
        schema = 'metrics'
        , alias = 'transfers_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
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
        , tx_hash
        , "from" as address
        , 'sent' as transfer_direction
        , (sum(amount_usd) * -1) as transfer_amount_usd
        , count(1) as transfer_count
    from
        {{ source('tokens', 'transfers') }}
    where
        1 = 1
    {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
    {% else %}
        and block_date >= timestamp '2024-09-01'
    {% endif %}
    group by
        blockchain
        , block_date
        , tx_hash
        , "from"
        , 'sent'

    union

    select
        blockchain
        , block_date
        , tx_hash
        , to as address
        , 'received' as transfer_direction
        , sum(amount_usd) as transfer_amount_usd
        , count(1) as transfer_count
    from
        {{ source('tokens', 'transfers') }}
    where
        1 = 1
    {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
    {% else %}
        and block_date >= timestamp '2024-09-01'
    {% endif %}
    group by
        blockchain
        , block_date
        , tx_hash
        , to
        , 'received'
), transfers_amount as (
    /*
    - create one column for transfer amount received, one column for transfer amount sent
    */
    select 
        blockchain
        , block_date
        , tx_hash
        , address
        , sum(case when transfer_direction = 'sent' then transfer_amount_usd else 0 end) as transfer_amount_usd_sent
        , sum(case when transfer_direction = 'received' then transfer_amount_usd else 0 end) as transfer_amount_usd_received
        , sum(transfer_count) as transfer_count
    from
        raw_transfers     
    group by
        blockchain
        , block_date
        , tx_hash
        , address
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
        , sum(coalesce(transfer_amount_usd_received, 0)) + sum(coalesce(transfer_amount_usd_sent, 0)) as transfer_amount_usd
        , sum(transfer_count) as transfer_count
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
    , sum(transfer_amount_usd_sent) as transfer_amount_usd_sent
    , sum(transfer_amount_usd_received) as transfer_amount_usd_received
    , sum(transfer_amount_usd) as transfer_amount_usd
    , sum(transfer_count) as transfer_count
from
    net_transfers
where
    transfer_amount_usd > 0
group by
    blockchain
    , block_date