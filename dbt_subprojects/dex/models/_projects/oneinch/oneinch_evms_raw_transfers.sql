{{  
    config(
        schema = 'oneinch_evms',
        alias = 'raw_transfers',
        materialized = 'view',
        unique_key = ['block_month', 'blockchain', 'tx_hash', 'call_trace_address', 'transfer_trace_address', 'transfer_contract_address'],
    )
}}

{% for blockchain in oneinch_blockchains_cfg_macro() if blockchain.exposed and blockchain.evm %}
    select * from {{ ref('oneinch_' + blockchain.name + '_raw_transfers') }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}