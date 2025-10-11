{% set substream = 'raw_calls' %}

{{  
    config(
        schema = 'oneinch_evms',
        alias = substream,
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
    )
}}

{% for stream, stream_data in oneinch_meta_cfg_macro()['streams'].items() %}
    {% for blockchain in stream_data['exposed'] if blockchain in oneinch_meta_cfg_macro()['blockchains']['evms'] %}
        select * from {{ ref('oneinch_' + blockchain + '_' + stream + '_' + substream) }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
    {% if not loop.last %}union all{% endif %}
{% endfor %}