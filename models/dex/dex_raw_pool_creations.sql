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


-- {topic0: params}
{%
    set config = {
        '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9': {
            'type': 'uniswap_compatible',
            'version': 'v2',
            'pool_position': 13,
        },
        '0x3541d8fea55be35f686281f975bf8b7ab8fbb500c1c7ddd6c4e714655e9cd4e2': {
            'type': 'uniswap_compatible',
            'version': 'v2',
            'pool_position': 13,
        },
        '0x41f8736f924f57e464ededb08bf71f868f9d142885bbc73a1516db2be21fc428': {
            'type': 'uniswap_compatible',
            'version': 'v2',
            'pool_position': 13,
        },
        '0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9': {
            'type': 'uniswap_compatible',
            'version': 'v2',
            'pool_position': 45,
        },
        '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118': {
            'type': 'uniswap_compatible',
            'version': 'v3',
            'pool_position': 45,
        },
    }
%}



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



with 


pool_created_logs as (
    {% for blockchain in blockchains %}
        {% for topic0, data in config.items() %}
            select 
                '{{ blockchain }}' as blockchain
                , '{{ data['type'] }}' as type
                , '{{ data['version'] }}' as version
                , substr(data, {{ config[topic0]['pool_position'] }}, 20) as pool
                , substr(topic1, 13) as token0
                , substr(topic2, 13) as token1
                , block_number
                , block_time
                , contract_address
                , tx_hash
            from {{ source(blockchain, 'logs') }}
            where topic0 = {{ topic0 }}
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)


, creation_traces as (
    {% for blockchain in blockchains %}
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
, _optimism_ovm1_legacy as (
    select 
        'optimism' as blockchain
        , 'uniswap_compatible' as type
        , 'v3' as version
        , pool
        , token0
        , token1 
        , creation_block_time
        , creation_block_number
        , contract_address
    from (
        select oldaddress as pool, * from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}
        union all
        select newaddress as pool, * from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}
    )
)



select 
    blockchain
    , type
    , version
    , pool
    , token0
    , token1
    , block_time as creation_block_time
    , block_number as creation_block_number
    , contract_address
from pool_created_logs
join creation_traces using(blockchain, tx_hash, block_number, block_time, pool)
{% if is_incremental() %}
    where {{ incremental_predicate('block_time') }}
{% endif %} 

union all

select * from _optimism_ovm1_legacy