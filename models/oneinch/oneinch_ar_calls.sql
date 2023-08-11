{{  
    config(
        schema = 'oneinch',
        alias = alias('ar_calls'),
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
        tags = ['dunesql'],
    )
}}



{% 
    set blockchains = [
        'arbitrum',
        'avalanche_c',
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
        'tx_success',
        'call_success',
        'call_trace_address',
        'caller',
        'call_selector',
        'call_input',
        'call_output'
    ]
%}



{% for blockchain in blockchains %}
    select {{ columns | join(', ') }} from {{ ref('oneinch_' + blockchain + '_ar_calls_transfers') }}
    group by {{ columns | join(', ') }}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}


