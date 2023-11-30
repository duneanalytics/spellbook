{{ 
    config(
        alias = 'eth', 
        schema = 'transfers_base',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['tx_hash', 'trace_address'],
        post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7", "chuxin","rantum"]\') }}'
    )
}}

select 
    'base' as blockchain
    ,r."from"
    ,r.to as wallet_address
    --Using the ETH placeholder address to match with prices tables
    ,0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token_address
    ,CAST(r.value as double) as amount_raw
    ,cast(r.value as double)/1e18 as value_decimal
    ,r.tx_hash
    ,r.trace_address
    ,r.block_time as block_time 
    ,r.block_number as tx_block_number 
    ,bytearray_substring(t.data, 1, 4) as tx_method_id
    ,t.to AS tx_to
    ,t."from" AS tx_from
    , CAST(date_trunc('month', r.block_time) as date) as block_month
from {{ source('base', 'traces') }} as r 
join {{ source('base', 'transactions') }} as t 
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