{% set stream = 'cc' %}

{{  
    config(
        schema = 'oneinch',
        alias = stream,
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}

{% set meta = oneinch_meta_cfg_macro()['blockchains'] %}

{% for blockchain, category in meta['category'].items() if category == 'evms' and blockchain in meta['exposed'] %}
    select * from {{ ref('oneinch_' + blockchain + '_' + stream) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}