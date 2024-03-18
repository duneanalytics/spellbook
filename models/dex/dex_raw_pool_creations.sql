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
    set uniswap_compatible_config = {
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
        }
    }
%}



{%
    set curvefi_compatible_base_config = {
        '0x52f2db69': {
            'type': 'curve_compatible',
            'version': 'Factory V1 Plain',
            'tokens_position': 4 + 1 + 32 * 2,
            'tokens_count': 4,
        },
        '0xd4b9e214': {
            'type': 'curve_compatible',
            'version': 'Factory V1 Plain',
            'tokens_position': 4 + 1 + 32 * 2,
            'tokens_count': 4,
        },
        '0xcd419bb5': {
            'type': 'curve_compatible',
            'version': 'Factory V1 Plain',
            'tokens_position': 4 + 1 + 32 * 2,
            'tokens_count': 4,
        },
        '0x5c16487b': {
            'type': 'curve_compatible',
            'version': 'Factory V1 Plain',
            'tokens_position': 4 + 1 + 32 * 2,
            'tokens_count': 4,
        },

        '0xc955fa04': {
            'type': 'curve_compatible',
            'version': 'Factory V2',
            'tokens_position': 4 + 1 + 32 * 2,
            'tokens_count': 2,
        },
        '0xaa38b385': {
            'type': 'curve_compatible',
            'version': 'Factory V2',
            'tokens_position': 4 + 1 + 32 * 2,
            'tokens_count': 3,
        },

        '0x5bcd3d83': {
            'type': 'curve_compatible',
            'version': 'Factory V1 Plain Stableswap',
            'tokens_position': 4 + 1 + 32 * 16,
            'tokens_count': 8,
        },

        
    }
%}



-- TODO: implement meta pools logic

{%
    set curvefi_compatible_meta_config = {
        '0xde7fe3bf': {
            'type': 'curve_compatible',
            'version': 'Factory V2 Meta',
            'base_pool_position': 4 + 1 + 13,
            'coin_position': 4 + 1 + 32 * 3 + 13,
        },
        '0xe339eb4f': {
            'type': 'curve_compatible',
            'version': 'Factory V2 Meta',
            'base_pool_position': 4 + 1 + 13,
            'coin_position': 4 + 1 + 32 * 3 + 13,
        },
    }
%}



{%
    set blockchains = [
        "ethereum",
        "bnb",
        "polygon",
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


uniswap_pool_created_logs as (
    {% for blockchain in blockchains %}
        {% for topic0, data in uniswap_compatible_config.items() %}
            select
                '{{ blockchain }}' as blockchain
                , '{{ data.type }}' as type
                , '{{ data.version }}' as version
                , substr(data, {{ data.pool_position }}, 20) as pool
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


, curve_base_pool_created_calls as (
    {% for blockchain in blockchains %}
        {% for selector, data in curvefi_compatible_base_config.items() %}
            select
                '{{ blockchain }}' as blockchain
                , '{{ data.type }}' as type
                , '{{ data.version }}' as version
                , substr(output, 13, 20) as pool
                , trace_address
                , transform(sequence(1, 32 * {{ data.tokens_count }}, 32), x -> substr(substr(substr(input, {{ data.tokens_position }}, 32 * {{ data.tokens_count }}), x, 32), 13)) tokens
                , block_number
                , block_time
                , "to" as contract_address
                , tx_hash
            from {{ source(blockchain, 'traces') }}
            where 
                substr(input, 1, 4) = {{ selector }} 
                and length(output) = 32
                and success 
                and tx_success 
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)

-- will be included later
, curve_meta_pool_created_calls as (
    {% for blockchain in blockchains %}
        {% for selector, data in curvefi_compatible_meta_config.items() %}
            select
                '{{ blockchain }}' as blockchain
                , '{{ data.type }}' as type
                , '{{ data.version }}' as version
                , substr(output, 13, 20) as pool
                , trace_address
                , substr(input, {{ data.base_pool_position }}, 20) as base_pool
                , substr(input, {{ data.coin_position }}, 20) as coin
                , block_number
                , block_time
                , "to" as contract_address
                , tx_hash
            from {{ source(blockchain, 'traces') }}
            where 
                substr(input, 1, 4) = {{ selector }} 
                and length(output) = 32
                and success 
                and tx_success 
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
, _uniswap_optimism_ovm1_legacy as (
    select
        'optimism' as blockchain
        , 'uniswap_compatible' as type
        , 'v3' as version
        , pool
        , token0
        , token1
        , array[token0, token1] as tokens
        , creation_block_time
        , creation_block_number
        , contract_address
    from (
        select oldaddress as pool, * from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}
        union all
        select newaddress as pool, * from {{ ref('uniswap_optimism_ovm1_pool_mapping') }}
    )
)

, pool_created_logs as (
    select 
        blockchain
        , type
        , version
        , pool
        , token0
        , token1
        , array[token0, token1] as tokens
        , block_time
        , block_number
        , contract_address
        , tx_hash
    from uniswap_pool_created_logs

    union all
    
    select 
        blockchain
        , type
        , version
        , pool
        , tokens[1] as token0
        , tokens[2] as token1
        , tokens
        , block_time
        , block_number
        , contract_address
        , tx_hash
    from curve_base_pool_created_calls
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
        , block_time as creation_block_time
        , block_number as creation_block_number
        , contract_address
    from pool_created_logs
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
