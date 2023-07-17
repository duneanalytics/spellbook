{{ 
    config(
	tags=['legacy'],
	
        materialized='incremental',
        alias = alias('eth_transfers', legacy_model=True),
        partition_by = ['block_date'],
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    ) 
}}

{% set project_start_date = '2021-11-17' %}

select 
    s.address,
    try_cast(date_trunc('day', et.block_time) as date) as block_date,
    et.block_time,
    -et.value as amount_raw,
    et.tx_hash,
    array_join(et.trace_address, ',') as trace_address
from {{ source('optimism', 'traces') }} et
inner join {{ ref('safe_optimism_safes_legacy') }} s on et.from = s.address
    and et.from != et.to -- exclude calls to self to guarantee unique key property
    and et.success = true
    and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
    and et.value > '0' -- value is of type string. exclude 0 value traces
{% if not is_incremental() %}
where et.block_time > '{{project_start_date}}' -- for initial query optimisation
{% endif %}
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
where et.block_time > date_trunc("day", now() - interval '10 days')
{% endif %}
        
union all
    
select 
    s.address, 
    try_cast(date_trunc('day', et.block_time) as date) as block_date,
    et.block_time,
    et.value as amount_raw,
    et.tx_hash,
    array_join(et.trace_address, ',') as trace_address
from {{ source('optimism', 'traces') }} et
inner join {{ ref('safe_optimism_safes_legacy') }} s on et.to = s.address
    and et.from != et.to -- exclude calls to self to guarantee unique key property
    and et.success = true
    and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
    and et.value > '0' -- value is of type string. exclude 0 value traces
{% if not is_incremental() %}
where et.block_time > '{{project_start_date}}' -- for initial query optimisation
{% endif %}
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
where et.block_time > date_trunc("day", now() - interval '10 days')
{% endif %}

union all
--ETH Transfers from deposits and withdrawals are ERC20 transfers of the 'deadeadead' ETH token. These do not appear in traces.

select 
    s.address, 
    try_cast(date_trunc('day', r.evt_block_time) as date) as block_date,
    r.evt_block_time as block_time,
    r.value as amount_raw,
    r.evt_tx_hash as tx_hash,
    cast(array(r.evt_index) as string) as trace_address
from {{ source('erc20_optimism', 'evt_Transfer') }} r
inner join {{ ref('safe_optimism_safes_legacy') }} s
    on r.to = s.address
where 
    r.contract_address = lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000')
    and r.value > '0'
    {% if not is_incremental() %}
    and r.evt_block_time > '{{project_start_date}}' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %} 
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    and r.evt_block_time >= date_trunc('day', now() - interval '10 days')
    {% endif %}

union all

select 
    s.address, 
    try_cast(date_trunc('day', r.evt_block_time) as date) as block_date,
    r.evt_block_time as block_time,
    -r.value as amount_raw,
    r.evt_tx_hash as tx_hash,
    cast(array(r.evt_index) as string) as trace_address
from {{ source('erc20_optimism', 'evt_Transfer') }} r
inner join {{ ref('safe_optimism_safes_legacy') }} s
    on r.from = s.address
where 
    r.contract_address = lower('0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000')
    and r.value > '0'
    {% if not is_incremental() %}
    and r.evt_block_time > '{{project_start_date}}' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %} 
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    and r.evt_block_time >= date_trunc('day', now() - interval '10 days')
    {% endif %}
