{{ 
    config(
        materialized='incremental',
        
        alias = 'eth_transfers',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz", "hosuke"]\') }}'
    ) 
}}

{% set project_start_date = '2021-11-17' %}

select
    t.*,
    p.price * t.amount_raw / 1e18 AS amount_usd

from (
    select 
        'optimism' as blockchain,
        'ETH' as symbol,
        s.address,
        try_cast(date_trunc('day', et.block_time) as date) as block_date,
        CAST(date_trunc('month', et.block_time) as DATE) as block_month,
        et.block_time,
        -CAST(et.value AS INT256) as amount_raw,
        et.tx_hash,
        array_join(et.trace_address, ',') as trace_address
    from {{ source('optimism', 'traces') }} et
    inner join {{ ref('safe_optimism_safes') }} s on et."from" = s.address
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
        'optimism' as blockchain,
        'ETH' as symbol,
        s.address, 
        try_cast(date_trunc('day', et.block_time) as date) as block_date,
        CAST(date_trunc('month', et.block_time) as DATE) as block_month,
        et.block_time,
        CAST(et.value AS INT256) as amount_raw,
        et.tx_hash,
        array_join(et.trace_address, ',') as trace_address
    from {{ source('optimism', 'traces') }} et
    inner join {{ ref('safe_optimism_safes') }} s on et.to = s.address
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
    --ETH Transfers from deposits and withdrawals are ERC20 transfers of the 'deadeadead' ETH token. These do not appear in traces.

    select 
        'optimism' as blockchain,
        'ETH' as symbol,
        s.address, 
        try_cast(date_trunc('day', r.evt_block_time) as date) as block_date,
        CAST(date_trunc('month', r.evt_block_time) as DATE) as block_month,
        r.evt_block_time as block_time,
        CAST(r.value AS INT256) as amount_raw,
        r.evt_tx_hash as tx_hash,
        cast(r.evt_index as varchar) as trace_address
    from {{ source('erc20_optimism', 'evt_Transfer') }} r
    inner join {{ ref('safe_optimism_safes') }} s
        on r.to = s.address
    where 
        r.contract_address = 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000
        and r.value > UINT256 '0'
        {% if not is_incremental() %}
        and r.evt_block_time > TIMESTAMP '{{project_start_date}}' -- for initial query optimisation
        {% endif %}
        {% if is_incremental() %} 
        -- to prevent potential counterfactual safe deployment issues we take a bigger interval
        and r.evt_block_time >= date_trunc('day', now() - interval '10' day)
        {% endif %}

    union all

    select 
        'optimism' as blockchain,
        'ETH' as symbol,
        s.address, 
        try_cast(date_trunc('day', r.evt_block_time) as date) as block_date,
        CAST(date_trunc('month', r.evt_block_time) as DATE) as block_month,
        r.evt_block_time as block_time,
        -CAST(r.value AS INT256) as amount_raw,
        r.evt_tx_hash as tx_hash,
        cast(r.evt_index as varchar) as trace_address
    from {{ source('erc20_optimism', 'evt_Transfer') }} r
    inner join {{ ref('safe_optimism_safes') }} s
        on r."from" = s.address
    where 
        r.contract_address = 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000
        and r.value > UINT256 '0'
        {% if not is_incremental() %}
        and r.evt_block_time > TIMESTAMP '{{project_start_date}}' -- for initial query optimisation
        {% endif %}
        {% if is_incremental() %} 
        -- to prevent potential counterfactual safe deployment issues we take a bigger interval
        and r.evt_block_time >= date_trunc('day', now() - interval '10' day)
        {% endif %}
) t

left join {{ source('prices', 'usd') }} p on p.blockchain is null
    and p.symbol = t.symbol
    and p.minute = date_trunc('minute', t.block_time)