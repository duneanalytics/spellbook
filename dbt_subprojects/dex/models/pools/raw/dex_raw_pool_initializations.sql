{{ config(
        schema='dex',
        alias = 'raw_pool_initializations',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'pool', 'tx_hash', 'call_trace_address']
)
}}



{% 
    set blockchains = [
        "ethereum", 
        "polygon", 
        "bnb", 
        "avalanche_c", 
        "gnosis", 
        "fantom", 
        "optimism", 
        "arbitrum", 
        "celo", 
        "base", 
        "zksync",
        "zora",
    ]
%}



select * from (
    {% for blockchain in blockchains %}
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
            {% endif %}

        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)

