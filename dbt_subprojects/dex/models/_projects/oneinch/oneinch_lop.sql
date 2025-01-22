{{  
    config(
        schema = 'oneinch',
        alias = 'lop',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}



{% for blockchain in oneinch_exposed_blockchains_list() %}
    select * from {{ ref('oneinch_' + blockchain + '_lop') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}