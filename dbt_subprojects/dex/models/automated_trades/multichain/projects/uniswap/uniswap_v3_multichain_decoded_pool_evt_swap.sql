{{ config(
        schema = 'uniswap_v3_multichain',
        alias = 'decoded_pool_evt_swap',
        partition_by = ['block_month', 'blockchain'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_number', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{%
    set blockchains = [
        "ethereum"
        , "arbitrum"
        , "base"
        , "bnb"
        , "fantom"
        , "gnosis"
        , "optimism"
        , "polygon"
        , "zkevm"
        , "zksync"
        , "zora"
    ]
%}

with pool_events as (
    {% for blockchain in blockchains %}      
        select 
            '{{blockchain}}' as blockchain,
            * 
        from (
            {{uniswap_v3_pool_event_decoding(
                logs = source(blockchain, 'logs')
            )}}
        )   
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)

select * from pool_events
