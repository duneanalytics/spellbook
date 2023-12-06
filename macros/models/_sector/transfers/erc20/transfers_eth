{% macro transfers_eth(blockchain, native_eth_address = 0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE) %}



with eth_transfers as (
    select
        r."from"
        ,r.to
        --Using the ETH deposit placeholder address to match with prices tables
        ,{{native_eth_address}} as contract_address
        ,cast(r.value as double) AS value
        ,cast(r.value as double)/1e18 as value_decimal
        ,r.tx_hash
        ,r.trace_address
        ,r.block_time as tx_block_time
        ,r.block_number as tx_block_number
        ,bytearray_substring(t.data, 1, 4) as tx_method_id
        ,t.to AS tx_to
        ,t."from" AS tx_from
    from {{ source(blockchain, 'traces') }} as r
    join {{ source(blockchain, 'transactions') }} as t
        on r.tx_hash = t.hash
        and r.block_number = t.block_number
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
              AND {{ incremental_predicate('t.block_time') }}
        {% endif %}
    where
        (r.call_type not in ('delegatecall', 'callcode', 'staticcall') or r.call_type is null)
        and r.tx_success
        and r.success
        and r.value > 0
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
              AND {{ incremental_predicate('r.block_time') }}
              AND {{ incremental_predicate('t.block_time') }}
        {% endif %}

-- Handle for Pre-Bedrock OP Bridged ETH ERC20  
{% if blockchain == 'optimism' %}
--ETH Transfers from deposits and withdrawals are ERC20 transfers of the 'deadeadead' ETH token. These do not appear in traces.
    union all
    

    select
        r."from"
        ,r.to
        --Using the ETH deposit placeholder address to match with prices tables
        ,0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as contract_address
        ,cast(r.value as double) AS value
        ,cast(r.value as double)/1e18 as value_decimal
        ,r.evt_tx_hash as tx_hash
        ,array[r.evt_index] as trace_address
        ,r.evt_block_time as tx_block_time
        ,r.evt_block_number as tx_block_number
        ,bytearray_substring(to_hex(t.data), 1, 4) as tx_method_id
        ,t.to AS tx_to
        ,t."from" AS tx_from
    from {{ source('erc20_' + blockchain, 'evt_transfer') }} as r
    join {{ source(blockchain, 'transactions') }} as t
        on r.evt_tx_hash = t.hash
        and r.evt_block_number = t.block_number
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
              AND {{ incremental_predicate('t.block_time') }}
        {% endif %}
    where
        r.contract_address = 0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000
        and t.success
        and r.value > 0
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
              AND {{ incremental_predicate('r.evt_block_time') }}
              AND {{ incremental_predicate('t.block_time') }}
        {% endif %}
        AND r.evt_block_time <= cast('2023-06-07' as timestamp) --day after Bedrock migration (June 6 2023 - with buffer)
        AND t.block_time <= cast('2023-06-07' as timestamp) --day after Bedrock migration (June 6 2023 - with buffer)
{% endif %}

)
select *
from eth_transfers

{% endmacro %}