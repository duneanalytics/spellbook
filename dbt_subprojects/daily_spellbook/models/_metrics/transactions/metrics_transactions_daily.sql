{{ config(
        schema = 'metrics'
        , alias = 'transactions_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with raw_tx as (
    select
        blockchain
        , cast(date_trunc('day', block_time) as date) as block_date
        , hash as tx_hash
    from
        {{ source('evms', 'transactions') }}
    where
        1 = 1
        and block_time >= timestamp '2024-10-18'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
        {% endif %}
    group by
        blockchain
        , cast(date_trunc('day', block_time) as date)
        , hash
), net_transfers_filter as (
    select 
        blockchain
        , block_date
        , tx_hash
    from
        {{ ref('metrics_net_transfers') }}
    where
        1 = 1
        and net_transfer_amount_usd >= 1 --only include tx's where transfer value is at least $1
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
), filtered_tx as (
    select
        tx.blockchain
        , tx.block_date
        , tx.tx_hash
    from
        raw_tx as tx
    inner join
        net_transfers_filter as nt
        on tx.blockchain = nt.blockchain
        and tx.block_date = nt.block_date
        and tx.tx_hash = nt.tx_hash
)
select
    blockchain
    , block_date
    , count(tx_hash) as tx_count
from
    filtered_tx
group by
    blockchain
    , block_date