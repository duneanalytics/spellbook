{{ 
    config(
        alias = alias('eth'), 
        tags = ['dunesql'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id',
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
        ,cast(r.value as double) AS value
        ,cast(r.value as double)/1e18 as value_decimal
        ,r.tx_hash
        ,r.trace_address
        ,r.block_time as tx_block_time 
        ,r.block_number as tx_block_number 
        ,bytearray_substring(t.data, 1, 4) as tx_method_id
        ,cast(r.tx_hash as varchar) || '-' || array_join(r.trace_address,',') as unique_transfer_id
        ,t.to AS tx_to
        ,t."from" AS tx_from
    from {{ source('ethereum', 'traces') }} as r 
    join {{ source('ethereum', 'transactions') }} as t 
        on r.tx_hash = t.hash
        and r.block_number = t.block_number
        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        and r.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    where 
        (r.call_type not in ('delegatecall', 'callcode', 'staticcall') or r.call_type is null)
        and r.tx_success = true
        and r.success = true
        and r.value > cast(0 as uint256)
        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        and r.block_time >= date_trunc('day', now() - interval '7' day)
        and t.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}