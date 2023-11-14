{{  
    config(
        schema = 'oneinch',
        alias = 'calls_classic',
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['suffix'],
        
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



with u as (
    {% for blockchain in blockchains %}
        select {{ select_columns }} from {{ ref('oneinch_' + blockchain + '_calls_transfers') }}
        group by {{ group_columns }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}    
)


select
    -- count(*) over(), blockchain, block_time, tx_hash, tx_from, tx_to, tx_success, call_success, call_trace_address, call_from, call_to, call_selector, protocol, call_input, call_output
    substr(call_input, length(call_input) - 3) as suffix
    -- , substr(call_input, 1, 4) as sf
    , count(*) cnt
    , count(distinct tx_from) as tx_from
    , count(distinct call_from) as call_from
    , count_if(call_success) as call_success
    , count(distinct call_to) as call_to
    , count(distinct call_selector) as call_selector
    , max(protocol) as protocol
    , max(length(call_output)) as length
    , count_if(tx_success) as tx_success
    , count(distinct tx_to) as tx_to
    -- , call_input
from u
where block_time >= timestamp '2023-06-01'
    and tx_success and call_success
group by 1
order by 2 desc




