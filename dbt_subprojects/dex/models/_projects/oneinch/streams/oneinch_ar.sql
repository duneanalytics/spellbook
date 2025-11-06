{% set stream = 'ar' %}

{{  
    config(
        schema = 'oneinch',
        alias = stream,
        materialized = 'view',
        unique_key = ['blockchain', 'block_month', 'tx_hash', 'call_trace_address'],
    )
}}

{% for blockchain in oneinch_blockchains_cfg_macro() if stream in blockchain.exposed %}
    select * from {{ ref('oneinch_' + blockchain.name + '_' + stream) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}