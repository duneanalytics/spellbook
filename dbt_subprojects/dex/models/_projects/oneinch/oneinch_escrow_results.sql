{{  
    config(
        schema = 'oneinch',
        alias = 'esrow_results',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'trace_address']
    )
}}



{% for blockchain in oneinch_exposed_blockchains_list() %}
    select * from {{ ref('oneinch_' + blockchain + '_escrow_results') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}