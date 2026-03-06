{%- macro stablecoins_tron_balances_from_transfers(transfers, start_date) -%}
-- transfers use token_address (varchar) and from_varchar/to_varchar

with

transfers_in as (
  select
    t.blockchain,
    t.block_date as day,
    t.block_time,
    t."to" as address,
    t.to_varchar as address_varchar,
    t.token_address,
    t.contract_address, -- token 0x contract address
    t.amount_raw as inflow,
    uint256 '0' as outflow
  from {{ transfers }} t
  where t."from" != t."to"
  {% if is_incremental() %}
  and {{ incremental_predicate('t.block_time') }}
  {% endif %}
),

transfers_out as (
  select
    t.blockchain,
    t.block_date as day,
    t.block_time,
    t."from" as address,
    t.from_varchar as address_varchar,
    t.token_address,
    t.contract_address, -- token 0x contract address
    uint256 '0' as inflow,
    t.amount_raw as outflow
  from {{ transfers }} t
  where t."from" != t."to"
  {% if is_incremental() %}
  and {{ incremental_predicate('t.block_time') }}
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
    max(address_varchar) as address_varchar,
    token_address,
    contract_address,
    sum(inflow) as daily_inflow,
    sum(outflow) as daily_outflow
  from all_flows
  group by 1, 2, 4, 6, 7
),

{% if is_incremental() %}
prior_balances as (
  select
    blockchain,
    address,
    address_varchar,
    token_address,
    contract_address,
    max(day) as day,
    max_by(last_updated, day) as last_updated,
    max_by(balance_raw, day) as balance_raw
  from {{ this }}
  where not {{ incremental_predicate('day') }}
  group by 1, 2, 3, 4, 5
),

{% endif %}

{% set uint256_max_double = '1.0e77' %}

changed_balances as (
  select
    blockchain,
    day,
    last_updated,
    address,
    address_varchar,
    token_address,
    contract_address,
    balance_raw,
    lead(cast(day as timestamp)) over (
      partition by address, token_address
      order by day
    ) as next_update_day
  from (
    select
      d.blockchain,
      d.day,
      d.last_updated,
      d.address,
      d.address_varchar,
      d.token_address,
      d.contract_address,
      cast(greatest(0e0, least({{ uint256_max_double }},
        {% if is_incremental() %}
        coalesce(cast(p.balance_raw as double), 0e0) +
        {% endif %}
        sum(cast(d.daily_inflow as double) - cast(d.daily_outflow as double)) over (
          partition by d.address, d.token_address
          order by d.day
          rows between unbounded preceding and current row
        )
      )) as uint256) as balance_raw
    from daily_aggregated d
    {% if is_incremental() %}
    left join prior_balances p
      on d.address = p.address
      and d.token_address = p.token_address
    {% endif %}
    {% if is_incremental() %}
    union all
    select
      p.blockchain,
      p.day,
      p.last_updated,
      p.address,
      p.address_varchar,
      p.token_address,
      p.contract_address,
      p.balance_raw
    from prior_balances p
    {% endif %}
  )
),

days as (
  select cast(timestamp as date) as day
  from {{ source('utils', 'days') }}
  where cast(timestamp as date) >= cast('{{ start_date }}' as date)
    and cast(timestamp as date) < current_date
  {% if is_incremental() %}
    and {{ incremental_predicate('cast(timestamp as date)') }}
  {% endif %}
),

forward_fill as (
  select
    b.blockchain,
    d.day,
    b.address,
    b.address_varchar,
    b.token_address,
    b.contract_address,
    b.balance_raw,
    b.last_updated
  from days d
  left join changed_balances b
    on d.day >= b.day
    and (b.next_update_day is null or cast(d.day as timestamp) < b.next_update_day)
)

select
  f.blockchain,
  f.day,
  f.address,
  f.address_varchar,
  f.token_address,
  f.contract_address,
  'trc20' as token_standard,
  cast(null as uint256) as token_id,
  f.balance_raw,
  f.last_updated
from forward_fill f
where (f.balance_raw > uint256 '0'
    or (f.balance_raw = uint256 '0' and cast(f.last_updated as date) = f.day))
  and f.address != 0x0000000000000000000000000000000000000000
{% if is_incremental() %}
  and {{ incremental_predicate('f.day') }}
{% endif %}

{%- endmacro %}
