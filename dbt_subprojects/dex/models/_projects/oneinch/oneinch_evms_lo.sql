{% set stream = 'lo' %}

{{  
    config(
        schema = 'oneinch_evms',
        alias = stream,
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
    )
}}

{% for blockchain, category in meta['blockchains']['category'].items() if category == 'evms' and blockchain in meta['blockchains']['exposed'] %}
    select * from {{ ref('oneinch_' + blockchain + '_' + stream) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}