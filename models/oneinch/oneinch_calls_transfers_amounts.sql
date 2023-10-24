{{  
    config(
        schema = 'oneinch',
        alias = 'calls_transfers_amounts',
        materialized = 'view',
        unique_key = ['blockchain', 'unique_call_transfer_id'],
        
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
    select * from {{ ref('oneinch_' + blockchain + '_calls_transfers') }}
    where 
        contract_address is not null
        and tx_success 
        and call_success

    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}