{% set stream = 'lo' %}
{% set substream = 'executions' %}

{{  
    config(
        schema = 'oneinch',
        alias = stream + '_' + substream,
        materialized = 'view',
        unique_key = ['blockchain', 'block_month', 'tx_hash', 'call_trace_address'],
    )
}}

{% for blockchain in oneinch_meta_cfg_macro()['streams'][stream]['exposed'] %}
    select * from {{ ref('oneinch_' + blockchain + '_' + stream + '_' + substream) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}