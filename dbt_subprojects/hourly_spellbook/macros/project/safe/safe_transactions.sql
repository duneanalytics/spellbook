{% macro safe_transactions(blockchain, project_start_date) %}

select
    '{{ blockchain }}' as blockchain,
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
from {{ source(blockchain, 'traces') }} tr
join {{ ref('safe_' ~ blockchain ~ '_safes') }} s
    on s.address = tr."from"
join {{ ref('safe_' ~ blockchain ~ '_singletons') }} ss
    on tr.to = ss.address
join {{ source(blockchain, 'transactions') }} et
    on tr.block_date = et.block_date
    and tr.tx_hash = et.hash
    and tr.block_number = et.block_number
where bytearray_substring(tr.input, 1, 4) in (
        0x6a761202, -- execTransaction
        0x468721a7, -- execTransactionFromModule
        0x5229073f -- execTransactionFromModuleReturnData
    )
    and tr.call_type = 'delegatecall'
    {% if not is_incremental() %}
    and tr.block_time > TIMESTAMP '{{ project_start_date }}' -- for initial query optimisation
    {% else %}
    and {{ incremental_predicate('tr.block_time') }}
    and {{ incremental_predicate('et.block_time')
    {% endif %}

{% endmacro %}
