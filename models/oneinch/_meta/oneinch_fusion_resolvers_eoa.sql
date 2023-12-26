{{
    config(
        schema = 'oneinch',
        alias = 'fusion_resolvers_eoa',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'address'],
    )
}}



{% set project_start_date = "timestamp '2022-12-25'" %} 



with
    
executors as (
    select blockchain, resolver_name, resolver_executor as "from"
    from {{ ref('oneinch_fusion_executors') }}
    group by 1, 2, 3
)

, settlements as (
    select blockchain, contract_address as "to"
    from {{ ref('oneinch_blockchains') }}, unnest(fusion_settlement_addresses) as t(contract_address)
)

, evms_traces as (
    {% for blockchain in oneinch_exposed_blockchains_list() %}
        select '{{ blockchain }}' as blockchain, tx_hash, "from", "to" from {{ source(blockchain, 'traces') }}
        {% if is_incremental() %}
            where block_time >= {{ incremental_predicate('block_time')}}
        {% else %}
            where block_time >= {{ project_start_date }}
        {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

, evms_transactions as (
    {% for blockchain in oneinch_exposed_blockchains_list() %}
        select '{{ blockchain }}' as blockchain, hash, "from" from {{ source(blockchain, 'transactions') }}
        {% if is_incremental() %}
            where block_time >= {{ incremental_predicate('block_time')}}
        {% else %}
            where block_time >= {{ project_start_date }}
        {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

, calls as (
    select blockchain, resolver_name, tx_hash as hash
    from evms_traces
    join executors using(blockchain, "from")
    join settlements using(blockchain, "to")
    group by 1, 2, 3
)

, txs as (
    select
        resolver_name
        , "from" as resolver_eoa
        , blockchain
    from evms_transactions
    join calls using(blockchain, hash)
    group by 1, 2, 3
)


select 
    blockchain
    , resolver_eoa as address
    , resolver_name as name
from txs