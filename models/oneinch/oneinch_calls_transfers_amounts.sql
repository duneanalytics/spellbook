{{  
    config(
        schema = 'oneinch',
        alias = alias('calls_transfers_amounts'),
        materialized = 'view',
        unique_key = ['blockchain', 'unique_call_transfer_id'],
        tags = ['dunesql'],
    )
}}



{% 
    set blockchains = [
        'arbitrum',
        'avalanche_c',
        'base',
        'ethereum',
        'fantom',
        'gnosis',
        'optimism',
        'polygon'
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