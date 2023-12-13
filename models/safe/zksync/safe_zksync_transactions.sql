{{ 
    config(
        materialized='incremental',
        schema = 'safe_zksync',
        alias = 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'], 
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["zksync"]\',
                                    "project",
                                    "safe",
                                    \'["kryptaki"]\') }}'
    ) 
}}

--add missing columns, remove unneeded columns from traces

with transactions as (
    select
        'zksync' as blockchain,
        try_cast(date_trunc('day', t.block_time) as date) as block_date,
        CAST(date_trunc('month', t.block_time) as DATE) as block_month,
        t.block_time,
        t.block_number,
        t.hash as tx_hash,
        s.address,
        t."from", --the address the Safe transacted to
        null as "to", --for other chains, this is the singleton address but on zksync this is the recipient address. Leaving this blank for now for consistency.
        t.gas_limit as gas, --in other chains, this is taken from traces, here we take from transactions
        null as execution_gas_used, --in other chains, this is taken from traces but not possible here due to duplicated traces with different estimates
        t.gas_used as total_gas_used,
        t.index as tx_index,
        t.success,
        t.data as input,
        null as "output",
        case
            when bytearray_substring(t.data, 1, 4) = 0x6a761202 then 'execTransaction'
            else 'unknown'
        end as method
    from {{ source('zksync', 'transactions') }} t
    join {{ ref('safe_zksync_safes') }} s
        on s.address = t.to --zksync execTransactions recorded as tx "to" the Safe, not "from"
    where 
        bytearray_substring(t.data, 1, 4) = 0x6a761202 -- execTransaction
        and t.success = true
        {% if not is_incremental() %}
        and t.block_time > TIMESTAMP '2023-09-01' -- for initial query optimisation
        {% endif %}
        {% if is_incremental() %}
        and t.block_time > date_trunc('day', now() - interval '7' day)
        {% endif %}
),

traces as (
select distinct --to remove duplicated traces
    'zksync' as blockchain,
    try_cast(date_trunc('day', tr.block_time) as date) as block_date,
    CAST(date_trunc('month', tr.block_time) as DATE) as block_month,
    tr.block_time,
    tr.block_number,
    tr.tx_hash,
    s.address,
    tr.value,
    null as sub_traces, --remove to avoid duplicates
    null as trace_address, --remove to avoid duplicates
    tr.error,
    tr.code,
    tr.tx_hash as "trace_tx_hash",
    tr.to, --used for merging
    tr.success --used for merging
from {{ source('zksync', 'traces') }} tr 
join {{ ref('safe_zksync_safes') }} s
    on s.address = tr."from"
where
    {% if not is_incremental() %}
    tr.block_time > TIMESTAMP '2023-09-01' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    tr.block_time > date_trunc('day', now() - interval '7' day)
    {% endif %}
)

select distinct
    t.blockchain,
    t.block_date,
    t.block_month,
    t.block_time,
    t.block_number,
    t.tx_hash,
    t.address,
    cast(t.to as varbinary) as "to", --for other chains, this is the singleton address, but not available in zksync.transactions or zksync.traces so keeping as null for consistency
    tr.value, --get value from traces (0 in transactions table)
    t.gas,
    cast(t.execution_gas_used as bigint) as execution_gas_used,
    t.total_gas_used,
    t.tx_index,
    cast(tr.sub_traces as bigint) as sub_traces,
    cast(tr.trace_address as array(bigint)) as trace_address,
    t.success,
    tr.error,
    tr.code,
    t.input, --get input from transactions (because contains the methodID)
    cast(t.output as varbinary) as "output",
    t.method,
    tr.tx_hash as trace_tx_hash --save the trace_tx_hash to match back on
    from transactions t
    inner join traces tr ON 
        tr.block_number = t.block_number
        AND tr.success = t.success
        AND tr.address = t.address --matching on safe address
        AND bytearray_length(bytearray_ltrim(tr.to)) > 4 --I think this identifies transactions from other traces
