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
        'block_time':'group',
        'tx_hash':'group',
        'tx_from':'any_value',
        'tx_to':'any_value',
        'tx_success':'group',
        'call_success':'group',
        'call_trace_address':'group',
        'call_from':'any_value',
        'call_to':'any_value',
        'call_selector':'any_value',
        'protocol':'any_value',
        'call_input':'any_value',
        'call_output':'any_value',
        'call_remains':'any_value'
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
