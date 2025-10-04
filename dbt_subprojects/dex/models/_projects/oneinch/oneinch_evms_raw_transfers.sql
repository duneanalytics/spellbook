{% set substream = 'raw_transfers' %}

{{  
    config(
        schema = 'oneinch_evms',
        alias = substream,
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address', 'transfer_trace_address', 'transfer_contract_address'],
    )
}}

{% for blockchain in oneinch_meta_cfg_macro()['blockchains']['evms'] %}
    select * from {{ ref('oneinch_' + blockchain + '_' + substream) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}