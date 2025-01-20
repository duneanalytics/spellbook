{{ config(
        schema = 'uniswap_v2_multichain',
        alias = 'decoded_pool_evt_swap',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

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
        "zora"
    ]
%}

with uniswap_pool_swap_logs as (
    {% for blockchain in blockchains %}      
        select 
            '{{blockchain}}' as blockchain,
            * 
        from (
            {{uniswap_v2_pool_event_decoding(
                logs = source(blockchain, 'logs')
            )}}
        )   
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
)

select * from uniswap_pool_swap_logs
