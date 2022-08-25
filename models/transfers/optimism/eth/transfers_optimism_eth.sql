{{ 
    config(
        alias ='eth', 
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
    )
}}
with eth_transfers as (
    select 
        r.from
        ,r.to
        --Using the ETH deposit placeholder address to match with prices tables
        ,lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000') as contract_address
        ,r.value
        ,r.value/1e18 as value_decimal
        ,r.tx_hash
        ,r.trace_address
        ,r.block_time as tx_block_time 
        ,r.block_number as tx_block_number 
        ,substring(t.data, 1, 10) as tx_method_id
        ,r.tx_hash || '-' || r.trace_address::string as unique_transfer_id
    from {{ source('optimism', 'traces') }} as r 
    join {{ source('optimism', 'transactions') }} as t 
        on r.tx_hash = t.hash
    where 
        (r.call_type not in ('delegatecall', 'callcode', 'staticcall') or r.call_type is null)
        and r.tx_success
        and r.success
        and r.value > 0 
        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        and r.block_time >= (select max(tx_block_time) - interval 2 days from {{ this }}) 
        {% endif %}

    union all 
    --ETH Transfers from deposits and withdrawals are ERC20 transfers of the 'deadeadead' ETH token. These do not appear in traces.

    select 
        r.from
        ,r.to
        --Using the ETH deposit placeholder address to match with prices tables
        ,lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000') as contract_address
        ,r.value
        ,r.value/1e18 as value_decimal
        ,r.evt_tx_hash as tx_hash
        ,array(r.evt_index) as trace_address
        ,r.evt_block_time as tx_block_time
        ,r.evt_block_number as tx_block_number
        ,substring(t.data, 1, 10) as tx_method_id
        ,r.evt_tx_hash || '-' || array(r.evt_index)::string as unique_transfer_id
    from {{ source('erc20_optimism', 'evt_transfer') }} as r
    join {{ source('optimism', 'transactions') }} as t 
        on r.evt_tx_hash = t.hash
    where 
        r.contract_address = lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000')
        and t.success
        and r.value > 0 
        {% if is_incremental() %} -- this filter will only be applied on an incremental run 
        and r.evt_block_time >= (select max(tx_block_time) - interval 2 days from {{ this }})
        {% endif %}
)
select * from eth_transfers order by tx_block_time
