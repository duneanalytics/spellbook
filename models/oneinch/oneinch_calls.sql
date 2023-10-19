{{  
    config(
        schema = 'oneinch',
        alias = alias('calls'),
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
        tags = ['dunesql'],
    )
}}



{% 
    set blockchains = [
        'arbitrum', 
        'avalanche_c',
        'base',
        'bnb',
        'ethereum',
        'fantom',
        'gnosis',
        'optimism',
        'polygon'
    ]
%}



{% 
    set columns = [
        'blockchain',
        'block_time',
        'tx_hash',
        'tx_from',
        'tx_to',
        'tx_success',
        'call_success',
        'call_trace_address',
        'call_from',
        'call_to',
        'call_selector',
        'protocol',
        'call_input',
        'call_output'
    ]
%}



{% for blockchain in blockchains %}
    select {{ columns | join(', ') }} from {{ ref('oneinch_' + blockchain + '_calls_transfers') }}
    group by {{ columns | join(', ') }}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
