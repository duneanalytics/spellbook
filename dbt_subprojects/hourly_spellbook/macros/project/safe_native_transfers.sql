{% macro safe_native_transfers(blockchain, native_token_symbol, project_start_date) %}

select
    t.*
    , p.price * t.amount_raw / 1e18 AS amount_usd
from
(
    select
        '{{ blockchain }}' as blockchain,
        '{{ native_token_symbol }}' as symbol,
        s.address,
        CAST(date_trunc('day', et.block_time) as date) as block_date,
        CAST(date_trunc('month', et.block_time) as DATE) as block_month,
        et.block_time,
        -CAST(et.value AS INT256) as amount_raw,
        et.tx_hash,
        array_join(et.trace_address, ',') as trace_address
    from
        {{ source(blockchain, 'traces') }} et
    join 
        {{ ref('safe_' ~ blockchain ~ '_safes') }} s
        on et."from" = s.address
    where
        et."from" != et.to -- exclude calls to self to guarantee unique key property
        and et.success = true
        and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
        and et.value > UINT256 '0' -- et.value is uint256 type
        {% if not is_incremental() %}
        and et.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% else %}
        and et.block_time > date_trunc('day', now() - interval '10' day) -- to prevent potential counterfactual safe deployment issues we take a bigger interval
        {% endif %}

    union all

    select
        '{{ blockchain }}' as blockchain,
        '{{ native_token_symbol }}' as symbol,
        s.address,
        CAST(date_trunc('day', et.block_time) as date) as block_date,
        CAST(date_trunc('month', et.block_time) as DATE) as block_month,
        et.block_time,
        CAST(et.value AS INT256) as amount_raw,
        et.tx_hash,
        array_join(et.trace_address, ',') as trace_address
    from
        {{ source(blockchain, 'traces') }} et
    join 
        {{ ref('safe_' ~ blockchain ~ '_safes') }} s
        on et.to = s.address
    where
        et."from" != et.to -- exclude calls to self to guarantee unique key property
        and et.success = true
        and (lower(et.call_type) not in ('delegatecall', 'callcode', 'staticcall') or et.call_type is null)
        and et.value > UINT256 '0' -- et.value is uint256 type
        {% if not is_incremental() %}
        and et.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% else %}
        and et.block_time > date_trunc('day', now() - interval '10' day) -- to prevent potential counterfactual safe deployment issues we take a bigger interval
        {% endif %}
) t
left join {{ source('prices', 'usd') }} p on p.blockchain is null
    and p.symbol = t.symbol
    and p.minute = date_trunc('minute', t.block_time)
    {% if not is_incremental() %}
    and p.minute >= TIMESTAMP '{{ project_start_date }}'
    {% else %}
    and p.minute > date_trunc('day', now() - interval '10' day) -- to prevent potential counterfactual safe deployment issues we take a bigger interval
    {% endif %}
{% endmacro %}