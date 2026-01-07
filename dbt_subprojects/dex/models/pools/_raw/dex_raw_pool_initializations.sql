{{ config(
        schema='dex',
        alias = 'raw_pool_initializations',
        materialized = 'incremental',
        tags = ['prod_exclude'],
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'pool', 'tx_hash', 'call_trace_address']
)
}}



select * from (
    -- only uni v2. re-initialization is restricted on v3
    select
        blockchain
        , 'uniswap_compatible' as type
        , 'v2' as version
        , block_time
        , "to" as pool
        , substr(input, 17, 20) token0
        , substr(input, 49, 20) token1
        , tx_hash
        , trace_address call_trace_address
    from {{ ref('dex_raw_pool_pre_materialized_traces') }}
    where 
        substr(input, 1, 4) = 0x485cc955 
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}
)

