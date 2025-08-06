{{ config(
        schema='dex',
        alias = 'raw_pool_initializations',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'pool', 'tx_hash', 'call_trace_address']
)
}}



select * from (
    {% for blockchain in dex_raw_pools_blockchains_macro() %}
        -- only uni v2. re-initialization is restricted on v3
        select
            '{{blockchain}}' as blockchain
            , 'uniswap_compatible' as type
            , 'v2' as version
            , block_time
            , "to" as pool
            , substr(input, 17, 20) token0
            , substr(input, 49, 20) token1
            , tx_hash
            , trace_address call_trace_address
        from {{ source(blockchain, 'traces') }}
        where 
            substr(input, 1, 4) = 0x485cc955 
            and success
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            -- {% else %}
            --     and block_time > date('2000-01-01')
            {% endif %}


        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)

