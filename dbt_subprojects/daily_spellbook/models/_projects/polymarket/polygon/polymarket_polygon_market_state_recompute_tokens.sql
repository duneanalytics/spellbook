{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_state_recompute_tokens',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['token_id']
  )
}}

with market_details as (
  select
    md.token_id,
    cast(
      coalesce(
        md.market_start_time_parsed,
        try_cast(md.market_start_time as timestamp),
        timestamp '2020-09-03 00:00:00'
      ) as date
    ) as market_start_day,
    md.active,
    md.closed,
    md.accepting_orders,
    md.outcome as market_outcome,
    md.resolved_on_timestamp
  from {{ ref('polymarket_polygon_market_details') }} as md
  where md.token_id is not null
),

ranked_market_details as (
  select
    token_id,
    market_start_day,
    active,
    closed,
    accepting_orders,
    market_outcome,
    resolved_on_timestamp,
    row_number() over (
      partition by token_id
      order by resolved_on_timestamp desc nulls last, market_start_day desc
    ) as rn
  from market_details
),

state as (
  select
    token_id,
    market_start_day,
    active,
    closed,
    accepting_orders,
    market_outcome,
    resolved_on_timestamp
  from ranked_market_details
  where rn = 1
),

{% if is_incremental() -%}
filtered as (
  select
    s.token_id,
    s.market_start_day,
    s.active,
    s.closed,
    s.accepting_orders,
    s.market_outcome,
    s.resolved_on_timestamp,
    case
      when prev.token_id is null then coalesce(s.resolved_on_timestamp, cast(s.market_start_day as timestamp), timestamp '1970-01-01 00:00:00')
      when coalesce(s.market_start_day, date '1970-01-01') <> coalesce(prev.market_start_day, date '1970-01-01') then current_timestamp
      when coalesce(s.active, '') <> coalesce(prev.active, '') then current_timestamp
      when coalesce(s.closed, '') <> coalesce(prev.closed, '') then current_timestamp
      when coalesce(s.accepting_orders, '') <> coalesce(prev.accepting_orders, '') then current_timestamp
      when coalesce(s.market_outcome, '') <> coalesce(prev.market_outcome, '') then coalesce(s.resolved_on_timestamp, current_timestamp)
      when coalesce(s.resolved_on_timestamp, timestamp '1970-01-01 00:00:00') <> coalesce(prev.resolved_on_timestamp, timestamp '1970-01-01 00:00:00') then coalesce(s.resolved_on_timestamp, current_timestamp)
      else prev.change_detected_at
    end as change_detected_at
  from state as s
  left join {{ this }} as prev
    on prev.token_id = s.token_id
  where prev.token_id is null
     or coalesce(s.market_start_day, date '1970-01-01') <> coalesce(prev.market_start_day, date '1970-01-01')
     or coalesce(s.active, '') <> coalesce(prev.active, '')
     or coalesce(s.closed, '') <> coalesce(prev.closed, '')
     or coalesce(s.accepting_orders, '') <> coalesce(prev.accepting_orders, '')
     or coalesce(s.market_outcome, '') <> coalesce(prev.market_outcome, '')
     or coalesce(s.resolved_on_timestamp, timestamp '1970-01-01 00:00:00') <> coalesce(prev.resolved_on_timestamp, timestamp '1970-01-01 00:00:00')
)
{% else -%}
filtered as (
  select
    s.token_id,
    s.market_start_day,
    s.active,
    s.closed,
    s.accepting_orders,
    s.market_outcome,
    s.resolved_on_timestamp,
    coalesce(s.resolved_on_timestamp, cast(s.market_start_day as timestamp), timestamp '1970-01-01 00:00:00') as change_detected_at
  from state as s
)
{% endif -%}

select
  token_id,
  market_start_day,
  active,
  closed,
  accepting_orders,
  market_outcome,
  resolved_on_timestamp,
  change_detected_at
from filtered
