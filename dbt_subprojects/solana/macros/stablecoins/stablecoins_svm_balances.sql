{%- macro stablecoins_svm_balances(
  blockchain,
  token_list,
  start_date
) %}

-- use uint256_max_double for safe double comparison
{% set uint256_max_double = '1.0e77' %}

with transfers_in as (
  select
    block_date as day,
    block_time,
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
    block_time,
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
    sum(outflow) as daily_outflow,
    max(block_time) as last_updated
  from all_flows
  group by 1, 2, 3
),

{% if is_incremental() %}
-- get last known balance for each address/token from before the incremental window
prior_balances as (
  select
    address,
    token_mint_address,
    max(day) as last_day,
    max_by(last_updated, day) as last_updated,
    max_by(balance_raw, day) as prior_balance
  from {{ this }}
  where not {{ incremental_predicate('day') }}
  group by 1, 2
),
{% endif %}

-- compute balances for addresses with new transfers
new_transfer_balances as (
  select
    d.day,
    d.last_updated,
    d.address,
    d.token_mint_address,
    cast(greatest(0e0, least({{ uint256_max_double }},
      {% if is_incremental() %}
      coalesce(cast(p.prior_balance as double), 0e0) +
      {% endif %}
      sum(cast(d.daily_inflow as double) - cast(d.daily_outflow as double)) over (
        partition by d.address, d.token_mint_address
        order by d.day
        rows between unbounded preceding and current row
      )
    )) as uint256) as balance_raw
  from daily_aggregated d
  {% if is_incremental() %}
  left join prior_balances p
    on d.address = p.address
    and d.token_mint_address = p.token_mint_address
  {% endif %}
),

-- combine new transfer balances with prior balances (for addresses without new transfers)
changed_balances as (
  select
    day,
    last_updated,
    address,
    token_mint_address,
    balance_raw,
    lead(day) over (
      partition by address, token_mint_address
      order by day
    ) as next_update_day
  from (
    select day, last_updated, address, token_mint_address, balance_raw
    from new_transfer_balances
    {% if is_incremental() %}
    union all
    -- include prior balances for addresses without new transfers (for forward-fill)
    select
      p.last_day as day,
      p.last_updated,
      p.address,
      p.token_mint_address,
      p.prior_balance as balance_raw
    from prior_balances p
    left join daily_aggregated d
      on p.address = d.address
      and p.token_mint_address = d.token_mint_address
    where d.address is null  -- no new transfers for this address/token
    {% endif %}
  )
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
    b.address,
    b.token_mint_address,
    b.balance_raw,
    b.last_updated
  from days d
  inner join changed_balances b
    on d.day >= b.day
    and (b.next_update_day is null or d.day < b.next_update_day)
)

select
  '{{blockchain}}' as blockchain,
  day,
  address,
  token_mint_address,
  balance_raw,
  last_updated
from forward_fill
where 1=1
  and (balance_raw > uint256 '0'
    or (balance_raw = uint256 '0' and cast(last_updated as date) = day))  -- keep actual zero-balance changes, not forward-fills
  -- exclude anomalous balances
  and not (address in (
    'BQhyvitcaYRYuyrkSacfP2aixjPsDqmhrt7uANjPcqZR',
    '5BpFBfXx5srPGtg3JGsumWARNh4UVagANgf7dGb1MYY1'
  ))
{% if is_incremental() %}
  and {{ incremental_predicate('day') }}
{% endif %}

{% endmacro %}
