{{ 
    config(
        alias = 'eth', 
        
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'trace_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7", "chuxin"]\') }}'
    )
}}

select 
    r."from"
    ,r.to
    --Using the ETH placeholder address to match with prices tables
    ,0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as contract_address
    ,r.value AS value
    ,cast(r.value as double)/1e18 as value_decimal
    ,r.tx_hash
    ,r.trace_address
    ,r.block_time as tx_block_time 
    ,r.block_number as tx_block_number 
    ,bytearray_substring(t.data, 1, 4) as tx_method_id
    ,t.to AS tx_to
    ,t."from" AS tx_from
from {{ source('ethereum', 'traces') }} as r 
join {{ source('ethereum', 'transactions') }} as t 
    on r.tx_hash = t.hash
    and r.block_number = t.block_number
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
    and {{incremental_predicate('t.block_time')}}
    {% endif %}
where 
    (r.call_type not in ('delegatecall', 'callcode', 'staticcall') or r.call_type is null)
    and r.tx_success = true
    and r.success = true
    and r.value > uint256 '0'
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
    and {{incremental_predicate('r.block_time')}}
    {% endif %}