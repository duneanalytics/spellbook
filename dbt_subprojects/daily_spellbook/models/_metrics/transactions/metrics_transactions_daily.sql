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

with net_transfers_filter as (
    select 
        blockchain
        , block_date
        , tx_hash
        , net_transfer_amount_usd
    from
        {{ ref('tokens_net_transfers') }}
    where
        1 = 1
        and net_transfer_amount_usd >= 1
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
), filtered_tx as (
    select
        tx.blockchain
        , date_trunc('day', tx.block_hour) as block_date
        , tx.tx_count
    from
        {{ source('evms', 'transaction_metrics') }} as tx
    inner join
        net_transfers_filter as nt
        on tx.blockchain = nt.blockchain
        and date_trunc('day', tx.block_hour) = nt.block_date
        and tx.tx_hash = nt.tx_hash
    where
        1 = 1
        and tx.block_hour >= timestamp '2024-10-01'
        {% if is_incremental() %}
        and {{ incremental_predicate('tx.block_hour') }}
        {% endif %}
)

select
    blockchain
    , block_date
    , sum(tx_count) as tx_count
from
    filtered_tx
group by
    blockchain
    , block_date