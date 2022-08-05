{{ 
    config(
        materialized='incremental',
        alias='eth_transfers',
        unique_key = 'address',
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge'
    ) 
}}

select 
    s.address,
    date_trunc('day', et.block_time) as day,
    -value as amount_raw
from {{ source('ethereum', 'traces') }} et
join {{ ref('safe_ethereum_safes') }} s on et.from = s.address
    and et.block_time > '2018-11-24' -- for initial query optimisation
    and success = true
    and (lower(call_type) not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    {% if is_incremental() %}
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    and et.block_time > (select max(day) from {{ this }}) - interval '10 days'
    {% endif %}
        
union all
    
select 
    s.address, 
    date_trunc('day', et.block_time) as day,
    value as amount_raw
from {{ source('ethereum', 'traces') }} et
join {{ ref('safe_ethereum_safes') }} s on et.to = s.address
    and et.block_time > '2018-11-24' -- for initial query optimisation
    and success = true
    and (lower(call_type) not in ('delegatecall', 'callcode', 'staticcall') or call_type is null)
    {% if is_incremental() %}
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    and et.block_time > (select max(day) from {{ this }}) - interval '10 days'
    {% endif %}
