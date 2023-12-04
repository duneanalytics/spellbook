{{
    config(
        materialized='incremental',
        
        schema='safe_celo',
        alias = 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida", "hosuke"]\') }}'
    )
}}

select
    'celo' as blockchain,
    try_cast(date_trunc('day', tr.block_time) as date) as block_date,
    CAST(date_trunc('month', tr.block_time) as DATE) as block_month,
    tr.block_time,
    tr.block_number,
    tr.tx_hash,
    s.address,
    tr.to,
    tr.value,
    tr.gas,
    tr.gas_used as execution_gas_used,
    et.gas_used as total_gas_used,
    tr.tx_index,
    tr.sub_traces,
    tr.trace_address,
    tr.success,
    tr.error,
    tr.code,
    tr.input,
    tr.output,
    case
        when bytearray_substring(tr.input, 1, 4) = 0x6a761202 then 'execTransaction'
        when bytearray_substring(tr.input, 1, 4) = 0x468721a7 then 'execTransactionFromModule'
        when bytearray_substring(tr.input, 1, 4) = 0x5229073f then 'execTransactionFromModuleReturnData'
        else 'unknown'
    end as method
from {{ source('celo', 'traces') }} tr
join {{ ref('safe_celo_safes') }} s
    on s.address = tr."from"
join {{ ref('safe_celo_singletons') }} ss
    on tr.to = ss.address
join {{ source('ethereum', 'transactions') }} et
    on tr.tx_hash = et.hash
    and tr.block_number = et.block_number
where bytearray_substring(tr.input, 1, 4) in (
        0x6a761202, -- execTransaction
        0x468721a7, -- execTransactionFromModule
        0x5229073f -- execTransactionFromModuleReturnData
    )
    and tr.call_type = 'delegatecall'
    {% if not is_incremental() %}
    and tr.block_time > TIMESTAMP '2021-06-20' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    and tr.block_time > date_trunc('day', now() - interval '7' day)
    {% endif %}
