{% macro uniswap_v3_blockchain_decoded_factory_evt(blockchain) %}

{{ config(
        schema = 'uniswap_v3_' ~ blockchain,
        alias = 'decoded_factory_evt',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

select 
    '{{blockchain}}' as blockchain,
    * 
from (
    {{uniswap_v3_factory_event_decoding(
        logs = source(blockchain, 'logs')
    )}}
)

{% endmacro %} 