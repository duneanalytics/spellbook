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

with evm as (
    select
        blockchain
        , block_date
        , approx_distinct(tx_hash) as tx_count --max 2% error, which is fine
    from
        {{ source('tokens', 'transfers') }}
    where
        1 = 1
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
)
, solana as (
    select
        'solana' as blockchain
        , block_date
        , approx_distinct(tx_id) as tx_count
    from
        {{ source('tokens_solana', 'transfers') }}
    where
        1 = 1
        and action != 'wrap'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        'solana'
        , block_date
)
, bitcoin as (
    select
        blockchain
        , block_date
        , approx_distinct(tx_id) as tx_count
    from
        {{ source('transfers_bitcoin', 'satoshi') }}
    where
        1 = 1
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
)
select
    blockchain
    , block_date
    , tx_count
from
    evm
union all
select
    blockchain
    , block_date
    , tx_count
from
    solana
union all
select
    blockchain
    , block_date
    , tx_count
from
    bitcoin