{{  
    config(
        schema = 'oneinch',
        alias = alias('ar_calls_transfers_amounts'),
        materialized = 'view',
        unique_key = ['blockchain', 'unique_call_transfer_id'],
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


{% for blockchain in blockchains %}
    select * from {{ ref('oneinch_' + blockchain + '_ar_calls_transfers') }}
    where 
        contract_address is not null
        and tx_success 
        and call_success
        {% if blockchain == 'bnb' %}
            and (rn_ta_asc <= 2 or rn_ta_desc <= 2)
        {% endif %}

    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}




