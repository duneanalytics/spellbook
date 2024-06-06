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
    select blockchain, contract_address
    from {{ ref('oneinch_blockchains') }}, unnest(fusion_settlement_addresses) as t(contract_address)
)

, evms_traces as (
    {% for blockchain in oneinch_exposed_blockchains_list() %}
        select '{{ blockchain }}' as blockchain, tx_hash, "from", "to" from {{ source(blockchain, 'traces') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% else %}
            where block_time >= {{ project_start_date }}
        {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

, evms_transactions as (
    {% for blockchain in oneinch_exposed_blockchains_list() %}
        select '{{ blockchain }}' as blockchain, hash, "from", block_time from {{ source(blockchain, 'transactions') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% else %}
            where block_time >= {{ project_start_date }}
        {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

, calls as (
    select evms_traces.blockchain, executors.resolver_name, executors.resolver_address, evms_traces.tx_hash as hash
    from evms_traces
    join executors on evms_traces.blockchain = executors.blockchain and evms_traces."from" = executors.resolver_executor
    join settlements on evms_traces.blockchain = settlements.blockchain and evms_traces."to" = settlements.contract_address
    group by 1, 2, 3, 4
)

, txs as (
    select
        resolver_name
        , evms_transactions."from" as resolver_eoa
        , resolver_address
        , blockchain
        , max(block_time) latest_tx_at
    from evms_transactions
    join calls using(blockchain, hash)
    group by 1, 2, 3, 4
)


select 
    blockchain
    , resolver_eoa as address
    , max_by(resolver_address, latest_tx_at) as resolver_address
    , max_by(resolver_name, latest_tx_at) as resolver_name
from txs
group by 1, 2