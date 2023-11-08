{{  
    config(
        schema = 'oneinch',
        alias = 'lop',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
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
        'polygon',
        'zksync'
    ]
%}



{% for blockchain in blockchains %}
    select * from {{ ref('oneinch_' + blockchain + '_lop') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}