{%- macro stablecoins_svm_balances_lead(
  blockchain,
  token_list,
  start_date
) %}

with transfers_in as (
  select
    block_date as day,
    to_owner as address,
    token_mint_address,
    amount_raw as inflow,
    uint256 '0' as outflow
  from {{ ref('stablecoins_' ~ blockchain ~ '_' ~ token_list ~ '_transfers') }}
  where to_owner is not null
    and block_date >= date '{{start_date}}'
  {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
  {% endif %}
),

transfers_out as (
  select
    block_date as day,
    from_owner as address,
    token_mint_address,
    uint256 '0' as inflow,
    amount_raw as outflow
  from {{ ref('stablecoins_' ~ blockchain ~ '_' ~ token_list ~ '_transfers') }}
  where from_owner is not null
    and block_date >= date '{{start_date}}'
  {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
  {% endif %}
),

all_flows as (
  select * from transfers_in
  union all
  select * from transfers_out
),

daily_aggregated as (
  select
    day,
    address,
    token_mint_address,
    sum(inflow) as daily_inflow,
    sum(outflow) as daily_outflow
  from all_flows
  group by 1, 2, 3
),

{% if is_incremental() %}
prior_balances as (
  select
    address,
    token_mint_address,
    max_by(cumulative_inflow, day) as prior_inflow,
    max_by(cumulative_outflow, day) as prior_outflow
  from {{ this }}
  where not {{ incremental_predicate('day') }}
  group by 1, 2
),
{% endif %}

cumulative_flows as (
  select
    d.day,
    d.address,
    d.token_mint_address,
    sum(d.daily_inflow) over (
      partition by d.address, d.token_mint_address
      order by d.day
      rows between unbounded preceding and current row
    ) as cumulative_inflow,
    sum(d.daily_outflow) over (
      partition by d.address, d.token_mint_address
      order by d.day
      rows between unbounded preceding and current row
    ) as cumulative_outflow,
    lead(d.day) over (
      partition by d.address, d.token_mint_address
      order by d.day
    ) as next_update_day
  from daily_aggregated d
),

days as (
  select cast(timestamp as date) as day
  from {{ source('utils', 'days') }}
  where cast(timestamp as date) >= date '{{start_date}}'
    and cast(timestamp as date) < current_date
  {% if is_incremental() %}
    and {{ incremental_predicate('cast(timestamp as date)') }}
  {% endif %}
),

forward_fill as (
  select
    d.day,
    c.address,
    c.token_mint_address,
    c.cumulative_inflow,
    c.cumulative_outflow,
    c.day as last_updated
  from days d
  inner join cumulative_flows c
    on d.day >= c.day
    and (c.next_update_day is null or d.day < c.next_update_day)
)

-- use slightly smaller value for safe double comparison
{% set uint256_max_double = '1.0e77' %}

select
  '{{blockchain}}' as blockchain,
  f.day,
  f.address,
  f.token_mint_address,
  f.cumulative_inflow,
  f.cumulative_outflow,
  cast(greatest(0e0, least({{ uint256_max_double }},
    {% if is_incremental() %}
    coalesce(cast(p.prior_inflow as double), 0e0) - coalesce(cast(p.prior_outflow as double), 0e0) +
    {% endif %}
    (cast(f.cumulative_inflow as double) - cast(f.cumulative_outflow as double))
  )) as uint256) as balance_raw,
  f.last_updated
from forward_fill f
{% if is_incremental() %}
left join prior_balances p
  on f.address = p.address
  and f.token_mint_address = p.token_mint_address
where {{ incremental_predicate('f.day') }}
  and cast(greatest(0e0, least({{ uint256_max_double }},
    coalesce(cast(p.prior_inflow as double), 0e0) - coalesce(cast(p.prior_outflow as double), 0e0) +
    (cast(f.cumulative_inflow as double) - cast(f.cumulative_outflow as double))
  )) as uint256) > uint256 '0'
{% else %}
where cast(greatest(0e0, least({{ uint256_max_double }},
  (cast(f.cumulative_inflow as double) - cast(f.cumulative_outflow as double))
)) as uint256) > uint256 '0'
{% endif %}

{% endmacro %}
