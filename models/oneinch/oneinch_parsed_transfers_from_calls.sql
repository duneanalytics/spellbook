{{  
    config(
        schema = 'oneinch',
        alias = 'parsed_transfers_from_calls',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address', 'transfer_trace_address']
    )
}}

-- View for all parsed transfers from calls, for internal usage. Won't be used in lineage.

{% for blockchain in all_evm_chains() %}
    {{ oneinch_parsed_transfers_from_calls_macro(blockchain) }}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
