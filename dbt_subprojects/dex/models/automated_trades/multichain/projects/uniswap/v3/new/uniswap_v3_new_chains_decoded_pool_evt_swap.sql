{{ config(
        schema = 'uniswap_v3_decoded_events',
        alias = 'new_chains_decoded_pool_evt_swap',
        partition_by = ['block_month', 'blockchain'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_number', 'tx_index', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{%
    set blockchains = uniswap_new_blockchains_list()
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