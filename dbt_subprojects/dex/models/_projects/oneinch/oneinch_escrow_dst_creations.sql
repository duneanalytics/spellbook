{{
    config(
        schema = 'oneinch',
        alias = 'escrow_dst_creations',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'tx_hash', 'trace_address']
    )
}}



{% for blockchain in oneinch_exposed_blockchains_list() %}
    {{ oneinch_escrow_dst_creations_macro(blockchain) }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}