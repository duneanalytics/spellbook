{% macro transfers_eth(blockchain, eth_placeholder_contract, base_traces, base_transactions, erc20_transfer ) %}

with eth_transfers as (
    select
        r."from"
        ,r.to
        --Using the ETH deposit placeholder address to match with prices tables
        ,{{eth_placeholder_contract}} as contract_address
        ,cast(r.value as double) AS value
        ,cast(r.value as double)/1e18 as value_decimal
        ,r.tx_hash
        ,r.trace_address
        ,r.block_time as tx_block_time
        ,r.block_number as tx_block_number
        ,substring(to_hex(t.data), 1, 10) as tx_method_id
        ,cast(r.tx_hash as varchar) || '-' || array_join(r.trace_address,',') as unique_transfer_id
        ,t.to AS tx_to
        ,t."from" AS tx_from
    from {{ base_traces }} as r
    join {{ base_transactions}} as t
        on r.tx_hash = t.hash
        and r.block_number = t.block_number
    where
        (r.call_type not in ('delegatecall', 'callcode', 'staticcall') or r.call_type is null)
        and r.tx_success
        and r.success
        and r.value > uint256 '0'
        {% if is_incremental() %}
        and {{incremental_predicate('r.block_time')}}
        and {{incremental_predicate('t.block_time')}}
        {% endif %}
    
    {%- if blockchain != 'ethereum' %}

    union all
    --ETH Transfers from deposits and withdrawals are ERC20 transfers of the 'deadeadead' ETH token. These do not appear in traces.

    select
        r."from"
        ,r.to
        --Using the ETH deposit placeholder address to match with prices tables
        ,{{eth_placeholder_contract}} as contract_address
        ,cast(r.value as double) AS value
        ,cast(r.value as double)/1e18 as value_decimal
        ,r.evt_tx_hash as tx_hash
        ,array[r.evt_index] as trace_address
        ,r.evt_block_time as tx_block_time
        ,r.evt_block_number as tx_block_number
        ,substring(to_hex(t.data), 1, 10) as tx_method_id
        ,cast(r.evt_tx_hash as varchar) || '-' || cast(r.evt_index as varchar) as unique_transfer_id
        ,t.to AS tx_to
        ,t."from" AS tx_from
    from {{ erc20_transfer }} as r
    join {{ base_transactions}} as t
        on r.evt_tx_hash = t.hash
        and r.evt_block_number = t.block_number
    where
        r.contract_address = {{eth_placeholder_contract}}
        and t.success
        and r.value > uint256 '0'
        {% if is_incremental() %}
        and {{incremental_predicate('r.evt_block_time')}}
        and {{incremental_predicate('t.block_time')}}
        {% endif %}
    
    {%- endif -%}
)
select *
from eth_transfers

{% endmacro %}
