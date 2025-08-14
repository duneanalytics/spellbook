{{ config(
        schema='dex',
        alias = 'raw_pool_creations',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'pool']
        )
}}



-- FYI:
-- -- about 20 pools on fantom missing because of broken creation_traces table
-- -- some of the pools on zksync are missing because of broken creation_traces table (known issue, node problems)

-- TODO:
-- -- implement mento pools on celo. it's only 11 of them, but implementation is not trivial, so for now we'll just skip them


with 

_pool_created_logs as (
    {% for topic0, data in dex_raw_pools_logs_config_macro().items() %}
        select
            blockchain
            , '{{ data.type }}' as type
            , '{{ data.version }}' as version
            , {{ data.pool }} as pool
            , {{ data.token0 }} as token0
            , {{ data.token1 }} as token1
            , {{ data.fee }} as fee
            , block_number
            , block_time
            , contract_address
            , tx_hash
        from {{ ref('dex_raw_pool_pre_materialized_logs') }}
        where topic0 = {{ topic0 }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)


, _pool_created_calls as (
    -- for curve
    {% for selector, data in dex_raw_pools_traces_config_macro().items() if not data.get('skip', False) and not data.get('initialization_call', False) %}
        select
            blockchain
            , '{{ data.type }}' as type
            , '{{ data.version }}' as version
            , {{ data.pool }} as pool
            , trace_address
            , {{ data.tokens }} as tokens
            , {{ data.fee }} as fee
            , block_number
            , block_time
            , "to" as contract_address
            , tx_hash
        from {{ ref('dex_raw_pool_pre_materialized_traces') }}
        where 
            substr(input, 1, 4) = {{ selector }} 
            and length(output) = 32 -- check this when adding new pools to config
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)


, creation_traces as (
    {% for blockchain in dex_raw_pools_blockchains_macro() %}
        select
            '{{ blockchain }}' as blockchain
            , address as pool
            , block_time
            , block_number
            , tx_hash
        from {{ source(blockchain, 'creation_traces') }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)


-- hardcoded OP legacy pools
, _uniswap_optimism_ovm1_legacy as (
    select
        'optimism' as blockchain
        , 'uniswap_compatible' as type
        , 'v3' as version
        , pool
        , token0
        , token1
        , array[token0, token1] as tokens
        , cast(fee as uint256) as fee
        , creation_block_time
        , creation_block_number
        , contract_address
    from (
        select oldaddress as pool, * from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}
        union all
        select newaddress as pool, * from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}
    )
)


, pools_cte as (
    select 
        blockchain
        , type
        , version
        , pool
        , token0
        , token1
        , array[token0, token1] as tokens
        , fee
        , block_time
        , block_number
        , contract_address
        , tx_hash
    from _pool_created_logs

    union all
    
    select 
        blockchain
        , type
        , version
        , pool
        , tokens[1] as token0
        , tokens[2] as token1
        , tokens
        , fee
        , block_time
        , block_number
        , contract_address
        , tx_hash
    from _pool_created_calls
)


, t as (
    select
        blockchain
        , type
        , version
        , pool
        , token0
        , token1
        , tokens
        , fee
        , block_time as creation_block_time
        , block_number as creation_block_number
        , contract_address
    from pools_cte
    join creation_traces using(blockchain, tx_hash, block_number, block_time, pool)
    {% if is_incremental() %}
        where {{ incremental_predicate('block_time') }}
    {% endif %}

    union all

    select * from _uniswap_optimism_ovm1_legacy
)


select * from (
    select 
        *
        , row_number() over(partition by blockchain, pool order by creation_block_time, contract_address) as rn
    from t
)
where rn = 1 -- remove duplicates // rare case, shitcoins only
