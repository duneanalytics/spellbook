{{  
    config(
        schema = 'oneinch',
        alias = 'project_orders',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}



{% for blockchain in oneinch_exposed_blockchains_list() %}
    select * from {{ ref('oneinch_' + blockchain + '_project_orders') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}