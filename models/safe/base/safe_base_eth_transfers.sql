{{
    config(
        materialized='incremental',
        
        alias= 'eth_transfers',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address', 'amount_raw'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["base"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida", "hosuke"]\') }}'
    )
}}

select
    t.*,
    p.price * t.amount_raw / 1e18 AS amount_usd

from (

    select
        'base' as blockchain,
        'ETH' as symbol,
        s.address,
        try_cast(date_trunc('day', et.block_time) as date) as block_date,
        CAST(date_trunc('month', et.block_time) as DATE) as block_month,
        et.block_time,
        -CAST(et.value AS INT256) as amount_raw,
        et.tx_hash,
        array_join(et.trace_address, ',') as trace_address
    from {{ source('base', 'traces') }} et
    join {{ ref('safe_base_safes') }} s on et."from" = s.address
        and et."from" != et.to -- exclude calls to self to guarantee unique key property
        and et.success = true
        and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
        and et.value > UINT256 '0' -- et.value is uint256 type
    {% if not is_incremental() %}
    where et.block_time > timestamp '2023-07-01' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    where et.block_time > date_trunc('day', now() - interval '10' day)
    {% endif %}

    union all

    select
        'base' as blockchain,
        'ETH' as symbol,
        s.address,
        try_cast(date_trunc('day', et.block_time) as date) as block_date,
        CAST(date_trunc('month', et.block_time) as DATE) as block_month,
        et.block_time,
        CAST(et.value AS INT256) as amount_raw,
        et.tx_hash,
        array_join(et.trace_address, ',') as trace_address
    from {{ source('base', 'traces') }} et
    join {{ ref('safe_base_safes') }} s on et.to = s.address
        and et."from" != et.to -- exclude calls to self to guarantee unique key property
        and et.success = true
        and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
        and et.value > UINT256 '0' -- et.value is uint256 type
    {% if not is_incremental() %}
    where et.block_time > timestamp '2023-07-01' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    where et.block_time > date_trunc('day', now() - interval '10' day)
    {% endif %}
) t

left join {{ source('prices', 'usd') }} p on p.blockchain is null
    and p.symbol = t.symbol
    and p.minute = date_trunc('minute', t.block_time)