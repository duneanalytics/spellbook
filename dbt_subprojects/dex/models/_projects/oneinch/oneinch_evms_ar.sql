{% set stream = 'ar' %}

{{  
    config(
        schema = 'oneinch_evms',
        alias = stream,
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
    )
}}

{% for blockchain, exposed in oneinch_meta_cfg_macro()['blockchains']['exposed'].items() if exposed == 'evms' %}
    select * from {{ ref('oneinch_' + blockchain + '_' + stream) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}