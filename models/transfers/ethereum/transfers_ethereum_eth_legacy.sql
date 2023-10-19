{{ 
    config(
	tags=['legacy'],
	
        alias = alias('eth', legacy_model=True), 
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
with eth_transfers as (
    select 
        r.from
        ,r.to
        --Using the ETH placeholder address to match with prices tables
        ,lower('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') as contract_address
        ,cast(r.value as double) AS value
        ,cast(r.value as double)/1e18 as value_decimal
        ,r.tx_hash
        ,r.trace_address
        ,r.block_time as tx_block_time 
        ,r.block_number as tx_block_number 
        ,substring(t.data, 1, 10) as tx_method_id
        ,r.tx_hash || '-' || cast(r.trace_address as string) as unique_transfer_id
        ,t.to AS tx_to
        ,t.`from` AS tx_from
    from {{ source('ethereum', 'traces') }} as r 
    join {{ source('ethereum', 'transactions') }} as t 
        on r.tx_hash = t.hash
        and r.block_number = t.block_number
    where 
        (r.call_type not in ('delegatecall', 'callcode', 'staticcall') or r.call_type is null)
        and r.tx_success
        and r.success
        and r.value > '0'
        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        and r.block_time >= date_trunc('day', now() - interval '1 week')
        and t.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
)
select *
from eth_transfers
;