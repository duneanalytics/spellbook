{{
    config(
        alias =alias('eth'),
        tags = ['dunesql'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id',
        post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7", "chuxin"]\') }}'
    )
}}
with eth_transfers as (
    select
        r."from"
        ,r.to
        --Using the ETH deposit placeholder address to match with prices tables
        ,0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000 as contract_address
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
    from {{ source('base', 'traces') }} as r
    join {{ source('base', 'transactions') }} as t
        on r.tx_hash = t.hash
        and r.block_number = t.block_number
    where
        (r.call_type not in ('delegatecall', 'callcode', 'staticcall') or r.call_type is null)
        and r.tx_success
        and r.success
        and r.value > uint256 '0'
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        and r.block_time >= date_trunc('day', now() - interval '7' day)
        and t.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

    union all
    --ETH Transfers from deposits and withdrawals are ERC20 transfers of the 'deadeadead' ETH token. These do not appear in traces.

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
        ,substring(to_hex(t.data), 1, 10) as tx_method_id
        ,cast(r.evt_tx_hash as varchar) || '-' || cast(r.evt_index as varchar) as unique_transfer_id
        ,t.to AS tx_to
        ,t."from" AS tx_from
    from {{ source('erc20_base', 'evt_transfer') }} as r
    join {{ source('base', 'transactions') }} as t
        on r.evt_tx_hash = t.hash
        and r.evt_block_number = t.block_number
    where
        r.contract_address = 0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000
        and t.success
        and r.value > uint256 '0'
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        and r.evt_block_time >= date_trunc('day', now() - interval '7' day)
        and t.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)
select *
from eth_transfers
