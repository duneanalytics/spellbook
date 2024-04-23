{{
    config(
        materialized='incremental',
        schema = 'safe_zkevm',
        alias= 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge',
        post_hook = '{{ expose_spells(
                        blockchains = \'["zkevm"]\',
                        spell_type = "project",
                        spell_name = "safe",
                        contributors = \'["danielpartida"]\') }}'
    )
}}

{% set project_start_date = '2023-09-01' %}

select
    'zkevm' as blockchain,
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
from {{ source('zkevm', 'traces') }} tr
join {{ ref('safe_zkevm_safes') }} s
    on s.address = tr."from"
join {{ ref('safe_zkevm_singletons') }} ss
    on tr.to = ss.address
join {{ source('zkevm', 'transactions') }} et
    on tr.tx_hash = et.hash
    {% if is_incremental() %}
    and {{ incremental_predicate('et.block_time') }}
    {% endif %}
    and tr.block_number = et.block_number
where bytearray_substring(tr.input, 1, 4) in (
        0x6a761202, -- execTransaction
        0x468721a7, -- execTransactionFromModule
        0x5229073f -- execTransactionFromModuleReturnData
    )
    and tr.call_type = 'delegatecall'
    {% if not is_incremental() %}
    and tr.block_time > TIMESTAMP '{{project_start_date}}' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    and {{ incremental_predicate('tr.block_time') }}
    {% endif %}
