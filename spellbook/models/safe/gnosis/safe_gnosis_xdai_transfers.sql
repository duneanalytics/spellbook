{{ 
    config(
        materialized='incremental',
        
        alias = 'xdai_transfers',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz", "hosuke"]\') }}'
    ) 
}}

{% set project_start_date = '2020-05-21' %}

select
    t.*,
    p.price * t.amount_raw / 1e18 AS amount_usd

from (
    select 
        'gnosis' as blockchain,
        'XDAI' as symbol,
        s.address,
        try_cast(date_trunc('day', et.block_time) as date) as block_date,
        CAST(date_trunc('month', et.block_time) as DATE) as block_month,
        et.block_time,
        -CAST(et.value AS INT256) as amount_raw,
        et.tx_hash,
        array_join(et.trace_address, ',') as trace_address
    from {{ source('gnosis', 'traces') }} et
    join {{ ref('safe_gnosis_safes') }} s on et."from" = s.address
        and et."from" != et.to -- exclude calls to self to guarantee unique key property
        and et.success = true
        and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
        and et.value > UINT256 '0' -- et.value is uint256 type
    {% if not is_incremental() %}
    where et.block_time > TIMESTAMP '{{project_start_date}}' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    where et.block_time > date_trunc('day', now() - interval '10' day)
    {% endif %}
            
    union all
        
    select 
        'gnosis' as blockchain,
        'XDAI' as symbol,
        s.address, 
        try_cast(date_trunc('day', et.block_time) as date) as block_date,
        CAST(date_trunc('month', et.block_time) as DATE) as block_month,
        et.block_time,
        CAST(et.value AS INT256) as amount_raw,
        et.tx_hash,
        array_join(et.trace_address, ',') as trace_address
    from {{ source('gnosis', 'traces') }} et
    join {{ ref('safe_gnosis_safes') }} s on et.to = s.address
        and et."from" != et.to -- exclude calls to self to guarantee unique key property
        and et.success = true
        and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
        and et.value > UINT256 '0' -- et.value is uint256 type
    {% if not is_incremental() %}
    where et.block_time > TIMESTAMP '{{project_start_date}}' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    where et.block_time > date_trunc('day', now() - interval '10' day)
    {% endif %}

    union all

    select 
        'gnosis' as blockchain,
        'XDAI' as symbol,
        s.address, 
        try_cast(date_trunc('day', a.evt_block_time) as date) as block_date,
        CAST(date_trunc('month', a.evt_block_time) as DATE) as block_month,
        a.evt_block_time as block_time, 
        CAST(a.amount AS INT256) as amount_raw,
        a.evt_tx_hash as tx_hash,
        cast(a.evt_index as varchar) as trace_address
    from {{ source('xdai_gnosis', 'BlockRewardAuRa_evt_AddedReceiver') }} a
    join {{ ref('safe_gnosis_safes') }} s
        on a.receiver = s.address
    {% if not is_incremental() %}
    where a.evt_block_time > TIMESTAMP '{{project_start_date}}' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    where a.evt_block_time > date_trunc('day', now() - interval '10' day)
    {% endif %}
) t

left join {{ source('prices', 'usd') }} p on p.blockchain is null
    and p.symbol = t.symbol
    and p.minute = date_trunc('minute', t.block_time)