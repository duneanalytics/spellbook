{{ 
    config(
        materialized='incremental',
        
        alias = 'eth_transfers',
        partition_by = ['block_month'],
        unique_key = ['address', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "safe",
                                    \'["sche", "tschubotz", "hosuke"]\') }}'
    ) 
}}

select
    t.*,
    p.price * t.amount_raw / 1e18 AS amount_usd

from (
    select 
        'ethereum' as blockchain,
        'ETH' as symbol,
        s.address,
        try_cast(date_trunc('day', et.block_time) as date) as block_date,
        CAST(date_trunc('month', et.block_time) as DATE) as block_month,
        et.block_time,
        -CAST(et.value AS INT256) as amount_raw,
        et.tx_hash,
        array_join(et.trace_address, ',') as trace_address
    from {{ source('ethereum', 'traces') }} et
    join {{ ref('safe_ethereum_safes') }} s on et."from" = s.address
        and et."from" != et.to -- exclude calls to self to guarantee unique key property
        and et.success = true
        and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
        and et.value > UINT256 '0'
    {% if not is_incremental() %}
    where et.block_time > TIMESTAMP '2018-11-24' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    where et.block_time > date_trunc('day', now() - interval '7' day)
    {% endif %}
            
    union all
        
    select 
        'ethereum' as blockchain,
        'ETH' as symbol,
        s.address, 
        try_cast(date_trunc('day', et.block_time) as date) as block_date,
        CAST(date_trunc('month', et.block_time) as DATE) as block_month,
        et.block_time,
        CAST(et.value AS INT256) as amount_raw,
        et.tx_hash,
        array_join(et.trace_address, ',') as trace_address
    from {{ source('ethereum', 'traces') }} et
    join {{ ref('safe_ethereum_safes') }} s on et.to = s.address
        and et."from" != et.to -- exclude calls to self to guarantee unique key property
        and et.success = true
        and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
        and et.value > UINT256 '0'
    {% if not is_incremental() %}
    where et.block_time > TIMESTAMP '2018-11-24' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    where et.block_time > date_trunc('day', now() - interval '7' day)
    {% endif %}
) t

left join {{ source('prices', 'usd') }} p on p.blockchain is null
    and p.symbol = t.symbol
    and p.minute = date_trunc('minute', t.block_time)