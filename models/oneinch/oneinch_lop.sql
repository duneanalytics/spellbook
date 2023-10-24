{{  
    config(
        schema = 'oneinch',
        alias = alias('lop'),
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
        tags = ['dunesql']
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



{% for blockchain in blockchains %}
    select * from {{ ref('oneinch_' + blockchain + '_lop') }}
    {% if not loop.last %} union all {% endif %}
{% endfor %}