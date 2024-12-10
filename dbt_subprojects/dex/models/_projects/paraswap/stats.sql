{%
set date_to = "DATE_TRUNC('day', CURRENT_TIMESTAMP)"
%}
{%
set date_from = "DATE_TRUNC('day', CURRENT_TIMESTAMP) - INTERVAL '1' day"
%}

{%
set delta_configs = [
    ['ethereum', 'delta-v1-single', 'paraswapdelta_ethereum.ParaswapDeltav1_call_settleSwap', 'contract_address', 'call_block_time', 'call_tx_hash', null],
    ['ethereum', 'delta-v1-batch', 'paraswapdelta_ethereum.ParaswapDeltav1_call_safeSettleBatchSwap', 'contract_address', 'call_block_time', 'call_tx_hash', null],

    ['ethereum', 'delta-v2', 'paraswapdelta_ethereum.ParaswapDeltav2_evt_OrderSettled', 'contract_address', 'evt_block_time', 'evt_tx_hash', null],
    ['base', 'delta-v2', 'paraswapdelta_base.ParaswapDeltav2_evt_OrderSettled', 'contract_address', 'evt_block_time', 'evt_tx_hash', null],

    ['ethereum', 'augustus', 'dex_aggregator.trades', 'project_contract_address', 'block_time', 'tx_hash', "project='paraswap'"],


    
    
]

%}  
    {% for blockchain, entity, table, contract_field_name, time_field_name, hash_field_name, conditional in delta_configs %}
        select '{{ entity }}' as entity, '{{ blockchain }}' as blockchain, {{contract_field_name}}, {{time_field_name}}, {{hash_field_name}} from {{ table }}
        where 
            ({{time_field_name}} BETWEEN {{date_from}} AND {{date_to}})
            {% if conditional %}
            AND {{ conditional }}
            {% endif %}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
