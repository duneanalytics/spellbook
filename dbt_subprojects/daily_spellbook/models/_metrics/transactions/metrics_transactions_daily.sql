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
                , hash as tx_hash
            from
                {{ source('evms', 'transactions') }}
            where
                1 = 1
                {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
                {% endif %}
        )
    union all
    select
        *
    from
        (
            select
                'tron' as blockchain
                , cast(date_trunc('day', block_time) as date) as block_date
                , hash as tx_hash
            from
                {{ source('tron', 'transactions') }}
            where
                1 = 1
                {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
                {% endif %}
        )
)
, solana as (
    select
        'solana' as blockchain
        , block_date
        , id as tx_hash
    from
        {{ source('solana', 'transactions') }}
    where
        1 = 1
        and block_date is not null --200m+ rows of nulls
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
)
select
    blockchain
    , block_date
    , count(tx_hash) as tx_count
from
    evm
group by
    blockchain
    , block_date
union all
select
    blockchain
    , block_date
    , count(tx_hash) as tx_count
from
    solana
group by
    blockchain
    , block_date
