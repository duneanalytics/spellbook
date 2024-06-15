{{  
    config(
        schema = 'oneinch',
        alias = 'project_swaps',
        materialized = 'view',
        unique_key = ['blockchain', 'block_number', 'tx_hash', 'call_trace_address', 'call_trade_id']
    )
}}



{% for blockchain in oneinch_exposed_blockchains_list() %}
    select * from {{ ref('oneinch_' + blockchain + '_project_swaps') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}