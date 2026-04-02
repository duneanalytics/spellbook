{% set chain = 'sui' %}
{% set start_date = '2023-04-12' %}
{% set usdc_coin_type = '0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::usdc' %}

{{
  config(
    tags = ['stablecoins'],
    schema = 'stablecoins_' ~ chain,
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with

transfers_filtered as (
  select
    t.blockchain,
    t.block_date as day,
    t.block_time,
    t."from" as from_address,
    t.to as to_address,
    t.amount_raw
  from {{ source('tokens_sui', 'transfers') }} t
  where lower(t.coin_type) = '{{ usdc_coin_type }}'
    and t.block_date >= date '{{ start_date }}'
    and t.amount_raw > uint256 '0'
  {% if is_incremental() -%}
    and {{ incremental_predicate('t.block_date') }}
  {% endif -%}
),

transfers_in as (
  select
    blockchain,
    day,
    block_time,
    to_address as address,
    amount_raw as inflow,
    uint256 '0' as outflow
  from transfers_filtered
  where to_address is not null
    and from_address != to_address
),

transfers_out as (
  select
    blockchain,
    day,
    block_time,
    from_address as address,
    uint256 '0' as inflow,
    amount_raw as outflow
  from transfers_filtered
  where from_address is not null
    and from_address != to_address
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
    sum(inflow) as daily_inflow,
    sum(outflow) as daily_outflow
  from all_flows
  group by 1, 2, 4
),

{% if is_incremental() -%}
prior_balances as (
  select
    blockchain,
    address,
    max(day) as day,
    max_by(last_updated, day) as last_updated,
    max_by(balance_raw, day) as balance_raw
  from {{ this }}
  where not {{ incremental_predicate('day') }}
  group by 1, 2
),
{% endif -%}

{% set uint256_max_double = '1.0e77' %}

changed_balances as (
  select
    blockchain,
    day,
    last_updated,
    address,
    balance_raw,
    lead(cast(day as timestamp)) over (
      partition by address
      order by day
    ) as next_update_day
  from (
    select
      d.blockchain,
      d.day,
      d.last_updated,
      d.address,
      cast(greatest(0e0, least({{ uint256_max_double }},
        {% if is_incremental() -%}
        coalesce(cast(p.balance_raw as double), 0e0) +
        {% endif -%}
        sum(cast(d.daily_inflow as double) - cast(d.daily_outflow as double)) over (
          partition by d.address
          order by d.day
        )
      )) as uint256) as balance_raw
    from daily_aggregated d
    {% if is_incremental() -%}
    left join prior_balances p
      on d.address = p.address
    {% endif -%}
    {% if is_incremental() -%}
    union all
    select
      p.blockchain,
      p.day,
      p.last_updated,
      p.address,
      p.balance_raw
    from prior_balances p
    {% endif -%}
  )
),

days as (
  select cast(timestamp as date) as day
  from {{ source('utils', 'days') }}
  where cast(timestamp as date) >= date '{{ start_date }}'
    and cast(timestamp as date) < current_date
  {% if is_incremental() -%}
    and {{ incremental_predicate('cast(timestamp as date)') }}
  {% endif -%}
),

forward_fill as (
  select
    b.blockchain,
    d.day,
    b.address,
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
  'USDC' as token_symbol,
  '{{ usdc_coin_type }}' as token_address,
  'coin' as token_standard,
  cast(null as uint256) as token_id,
  f.balance_raw,
  cast(f.balance_raw as double) / power(10, 6) as balance,
  cast(f.balance_raw as double) / power(10, 6) as balance_usd,
  'USD' as currency,
  f.last_updated
from forward_fill f
where (f.balance_raw > uint256 '0'
  or (f.balance_raw = uint256 '0' and cast(f.last_updated as date) = f.day))
{% if is_incremental() -%}
  and {{ incremental_predicate('f.day') }}
{% endif -%}
