{{
    config(
        schema = 'oneinch',
        alias = 'fusion_accounts',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'address'],
    )
}}



{% set project_start_date = "timestamp '2022-12-25'" %} 



with
    
executors as (
    select blockchain, name as resolver_name, resolver_executor, resolver_address
    from {{ ref('oneinch_fusion_executors') }}
    join {{ ref('oneinch_fusion_resolvers') }} on address = resolver_address
    group by 1, 2, 3, 4
)

, settlements as (
    select blockchain, settlement
    from {{ ref('oneinch_blockchains') }}, unnest(fusion_settlement_addresses) as t(settlement)
)

, evms_traces as (
    {% for blockchain in oneinch_exposed_blockchains_list() %}
        select
            '{{ blockchain }}' as blockchain
            , tx_hash
            , "from" as resolver_executor
            , "to" as settlement
        from {{ source(blockchain, 'traces') }}
        where
            {% if is_incremental() %}
                {{ incremental_predicate('block_time') }}
            {% else %}
                block_time >= {{ project_start_date }}
            {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

, fusion as (
    select blockchain, tx_hash, call_from as resolver_executor
    from {{ ref('oneinch_lop') }}
    where
        {% if is_incremental() %}
            {{ incremental_predicate('block_time') }}
        {% else %}
            block_time >= {{ project_start_date }}
        {% endif %}
        and flags['fusion']
)

, calls as (
    select blockchain, resolver_name, resolver_address, tx_hash
    from evms_traces
    join executors using(blockchain, resolver_executor)
    join settlements using(blockchain, settlement)
    group by 1, 2, 3, 4

    union all

    select blockchain, resolver_name, resolver_address, tx_hash
    from fusion
    join executors using(blockchain, resolver_executor)
)

, evms_transactions as (
    {% for blockchain in oneinch_exposed_blockchains_list() %}
        select
            '{{ blockchain }}' as blockchain
            , hash as tx_hash
            , "from" as tx_from
            , block_time
        from {{ source(blockchain, 'transactions') }}
        where
            {% if is_incremental() %}
                {{ incremental_predicate('block_time') }}
            {% else %}
                block_time >= {{ project_start_date }}
            {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

, txs as (
    select
        resolver_name
        , tx_from as resolver_eoa
        , resolver_address
        , blockchain
        , max(block_time) as latest_tx_at
    from evms_transactions
    join calls using(blockchain, tx_hash)
    group by 1, 2, 3, 4
)

-- output --

select
    blockchain
    , resolver_eoa as address
    , max_by(resolver_address, latest_tx_at) as resolver_address
    , max_by(resolver_name, latest_tx_at) as resolver_name
from txs
group by 1, 2