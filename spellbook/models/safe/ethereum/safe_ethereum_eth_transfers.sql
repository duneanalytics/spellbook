{{ 
    config(
        materialized='incremental',
        alias='eth_transfers',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge'
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
    and success = true
    and (lower(call_type) not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)    
where et.block_time > '2018-11-24' -- for initial query optimisation
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
and et.block_time > (select max(block_time) from {{ this }}) - interval '10 days'
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
    and success = true
    and (lower(call_type) not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
where et.block_time > '2018-11-24' -- for initial query optimisation
{% if is_incremental() %}
-- to prevent potential counterfactual safe deployment issues we take a bigger interval
and et.block_time > (select max(block_time) from {{ this }}) - interval '10 days'
{% endif %}