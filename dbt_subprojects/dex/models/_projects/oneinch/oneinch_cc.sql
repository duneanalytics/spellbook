{% set stream = 'cc' %}

{{  
    config(
        schema = 'oneinch',
        alias = stream,
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}

{% for blockchain, category in oneinch_meta_cfg_macro()['blockchains']['category'].items() if category == 'evms' and blockchain in oneinch_meta_cfg_macro()['blockchains']['exposed'] %}
    select * from {{ ref('oneinch_' + blockchain + '_' + stream) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}