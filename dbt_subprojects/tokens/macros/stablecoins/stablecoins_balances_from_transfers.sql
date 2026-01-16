{%- macro stablecoins_balances_from_transfers(transfers) %}

with transfers_in as (
  select
    blockchain,
    block_date as day,
    block_time,
    "to" as address,
    token_address,
    amount_raw as inflow,
    uint256 '0' as outflow
  from {{ transfers }}
  {% if is_incremental() %}
  where {{ incremental_predicate('block_time') }}
  {% endif %}
),

transfers_out as (
  select
    blockchain,
    block_date as day,
    block_time,
    "from" as address,
    token_address,
    uint256 '0' as inflow,
    amount_raw as outflow
  from {{ transfers }}
  {% if is_incremental() %}
  where {{ incremental_predicate('block_time') }}
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
    max(block_time) as last_updated,
    address,
    token_address,
    sum(inflow) as daily_inflow,
    sum(outflow) as daily_outflow
  from all_flows
  group by 1, 2, 4, 5
),

{% if is_incremental() %}
prior_balances as (
  select
    address,
    token_address,
    max_by(balance_raw, day) as prior_balance
  from {{ this }}
  where not {{ incremental_predicate('last_updated') }}
  group by 1, 2
),
{% endif %}

cumulative_flows as (
  select
    d.blockchain,
    d.day,
    d.last_updated,
    d.address,
    d.token_address,
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
  c.address,
  c.token_address,
  'erc20' as token_standard,
  cast(null as uint256) as token_id,
  -- clamp to [0, uint256_max] to safely cast to uint256
  cast(greatest(0e0, least({{ uint256_max_double }},
    {% if is_incremental() %}
    coalesce(cast(p.prior_balance as double), 0e0) +
    {% endif %}
    (cast(c.cumulative_inflow as double) - cast(c.cumulative_outflow as double))
  )) as uint256) as balance_raw,
  c.last_updated
from cumulative_flows c
{% if is_incremental() %}
left join prior_balances p
  on c.address = p.address
  and c.token_address = p.token_address
where {{ incremental_predicate('c.last_updated') }}
{% endif %}

{% endmacro %}
