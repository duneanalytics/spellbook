{{ 
    config(
        materialized='incremental',
        alias='eth_transfers',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'tx_hash', 'trace_address', 'amount_raw'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "safe",
                                    \'["sche"]\') }}'
    ) 
}}

select 
    s.address,
    try_cast(date_trunc('day', et.block_time) as date) as block_date,
    et.block_time,
    -value as amount_raw,
    et.tx_hash,
    array_join(et.trace_address, ',') as trace_address
from {{ source('ethereum', 'traces') }} et
join {{ ref('safe_ethereum_safes') }} s on et.from = s.address
    and et.from != et.to -- exclude calls to self to guarantee unique key property
    and et.success = true
    and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
    and et.value > 0 -- exclue 0 value traces
{% if not is_incremental() %}
where et.block_time > '2018-11-24' -- for initial query optimisation    
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
    value as amount_raw,
    et.tx_hash,
    array_join(et.trace_address, ',') as trace_address
from {{ source('ethereum', 'traces') }} et
join {{ ref('safe_ethereum_safes') }} s on et.to = s.address
    and et.from != et.to -- exclude calls to self to guarantee unique key property
    and et.success = true
    and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
    and et.value > 0 -- exclue 0 value traces
{% if not is_incremental() %}
where et.block_time > '2018-11-24' -- for initial query optimisation    
{% endif %}
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
where et.block_time > date_trunc("day", now() - interval '10 days')
{% endif %}