{{  
    config(
        schema = 'oneinch',
        alias = 'call_transfers',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address', 'transfer_trace_address', 'transfer_native'],
    )
}}



{% for blockchain in oneinch_exposed_blockchains_list() %}
    select * from {{ ref('oneinch_' + blockchain + '_call_transfers') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}