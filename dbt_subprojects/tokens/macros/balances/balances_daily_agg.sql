{%- macro balances_daily_agg(balances_raw) %}
select
    *,
    {{ dbt_utils.generate_surrogate_key(['day', 'address', 'token_address', 'token_standard', 'token_id']) }} as unique_key
from (
    select
        blockchain,
        cast(date_trunc('day', block_time) as date) as day,
        block_number,
        block_time,
        address,
        token_address,
        token_standard,
        token_id,
        balance_raw,
        row_number() OVER (partition by date_trunc('day', block_time), token_standard, address, token_address, token_id order by block_number desc) as row_number
    from {{ balances_raw}} balances
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
) where row_number = 1


{% endmacro %}


{%- macro balances_daily_agg_from_transfers(transfers) %}

with transfers_in as (
    select
        blockchain,
        block_date as day,
        block_number,
        block_time,
        "to" as address,
        contract_address as token_address,
        token_standard,
        amount_raw as inflow,
        uint256 '0' as outflow
    from {{ transfers }}
    where "to" != 0x0000000000000000000000000000000000000000
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% endif %}
),

transfers_out as (
    select
        blockchain,
        block_date as day,
        block_number,
        block_time,
        "from" as address,
        contract_address as token_address,
        token_standard,
        uint256 '0' as inflow,
        amount_raw as outflow
    from {{ transfers }}
    where "from" != 0x0000000000000000000000000000000000000000
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% endif %}
),

all_flows as (
    select * from transfers_in
    union all
    select * from transfers_out
),

daily_aggregated as (
    select
        blockchain,
        day,
        max(block_number) as block_number,
        max(block_time) as block_time,
        address,
        token_address,
        token_standard,
        sum(inflow) as daily_inflow,
        sum(outflow) as daily_outflow
    from all_flows
    group by 1, 2, 5, 6, 7
),

{% if is_incremental() %}
prior_balances as (
    select
        address,
        token_address,
        token_standard,
        max_by(balance_raw, day) as prior_balance
    from {{ this }}
    where not {{ incremental_predicate('block_time') }}
    group by 1, 2, 3
),
{% endif %}

cumulative_flows as (
    select
        d.blockchain,
        d.day,
        d.block_number,
        d.block_time,
        d.address,
        d.token_address,
        d.token_standard,
        sum(d.daily_inflow) over (
            partition by d.address, d.token_address
            order by d.day
            rows between unbounded preceding and current row
        ) as cumulative_inflow,
        sum(d.daily_outflow) over (
            partition by d.address, d.token_address
            order by d.day
            rows between unbounded preceding and current row
        ) as cumulative_outflow
    from daily_aggregated d
)

-- use slightly smaller value for safe double comparison
{% set uint256_max_double = '1.0e77' %}

select
    c.blockchain,
    c.day,
    c.block_number,
    c.block_time,
    c.address,
    c.token_address,
    c.token_standard,
    cast(null as uint256) as token_id,
    -- clamp to [0, uint256_max] to safely cast to uint256
    cast(greatest(0e0, least({{ uint256_max_double }},
        {% if is_incremental() %}
        coalesce(cast(p.prior_balance as double), 0e0) +
        {% endif %}
        (cast(c.cumulative_inflow as double) - cast(c.cumulative_outflow as double))
    )) as uint256) as balance_raw,
    {{ dbt_utils.generate_surrogate_key(['c.day', 'c.address', 'c.token_address', 'c.token_standard']) }} as unique_key
from cumulative_flows c
{% if is_incremental() %}
left join prior_balances p
    on c.address = p.address
    and c.token_address = p.token_address
    and c.token_standard = p.token_standard
where {{ incremental_predicate('c.block_time') }}
{% endif %}

{% endmacro %}
