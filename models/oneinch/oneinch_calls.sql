{{  
    config(
        schema = 'oneinch',
        alias = 'calls',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}



{% for blockchain in oneinch_exposed_blockchains_list() %}
    select * from ({{ oneinch_calls_macro(blockchain) }})
    {% if not loop.last %} union all {% endif %}
{% endfor %}