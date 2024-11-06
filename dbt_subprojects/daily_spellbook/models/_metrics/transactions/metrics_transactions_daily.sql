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
        *
    from
        (
            select
                blockchain
                , cast(date_trunc('day', block_time) as date) as block_date
                , count(hash) as tx_count
            from
                {{ source('evms', 'transactions') }}
            where
                1 = 1
                {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
                {% endif %}
            group by
                blockchain
                , cast(date_trunc('day', block_time) as date)
        )
    union all
    select
        *
    from
        (
            select
                'tron' as blockchain
                , cast(date_trunc('day', block_time) as date) as block_date
                , count(hash) as tx_count
            from
                {{ source('tron', 'transactions') }}
            where
                1 = 1
                {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
                {% endif %}
            group by
                'tron'
                , cast(date_trunc('day', block_time) as date)
        )
)
, solana as (
    select
        'solana' as blockchain
        , block_date
        , count(id) as tx_count
    from
        {{ source('solana', 'transactions') }}
    where
        1 = 1
        and block_date is not null --200m+ rows of nulls
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        'solana'
        , block_date
)
, bitcoin as (
    select
        'bitcoin' as blockchain
        , date as block_date
        , sum(transaction_count) as tx_count
    from
        {{ source('bitcoin', 'blocks') }}
    where
        1 = 1
        {% if is_incremental() %}
        and {{ incremental_predicate('date') }}
        {% endif %}
    group by
        'bitcoin'
        , date
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