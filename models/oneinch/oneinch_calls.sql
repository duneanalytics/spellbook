{{  
    config(
        schema = 'oneinch',
        alias = 'calls',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
        
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



{% 
    set columns = {
        'blockchain':'group',
        'block_time':'max',
        'tx_hash':'group',
        'tx_from':'max',
        'tx_to':'max',
        'tx_success':'max',
        'call_success':'max',
        'call_trace_address':'group',
        'call_from':'max',
        'call_to':'max',
        'call_selector':'max',
        'protocol':'max',
        'call_input':'max',
        'call_output':'max'
    }
%}



{% set select_columns = [] %}
{% set group_columns = [] %}
{% for key, value in columns.items() %}
    {% if value == "group" %}
        {% set select_columns = select_columns.append(key) %}
        {% set group_columns = group_columns.append(key) %}
    {% else %}
        {% set select_columns = select_columns.append(value + '(' + key + ') as ' + key) %}
    {% endif %}
{% endfor %}
{% set select_columns = select_columns | join(', ') %}
{% set group_columns = group_columns | join(', ') %}



{% for blockchain in blockchains %}
    select {{ select_columns }} from {{ ref('oneinch_' + blockchain + '_calls_transfers') }}
    group by {{ group_columns }}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
