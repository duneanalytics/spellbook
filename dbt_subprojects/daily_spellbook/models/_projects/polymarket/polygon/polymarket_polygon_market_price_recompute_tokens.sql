{{ config(
    schema = 'polymarket_polygon',
    alias = 'market_price_recompute_tokens',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['token_id']
  )
}}

-- derive current token-level recompute state from market metadata
with market_details as (
  select
    md.token_id,
    coalesce(
      cast(date_trunc('hour', cast(try(from_iso8601_timestamp(md.market_end_time)) as timestamp)) as timestamp),
      cast(date_trunc('hour', try_cast(substring(md.market_end_time from 1 for 19) as timestamp)) as timestamp),
      cast(date_trunc('hour', md.resolved_on_timestamp) as timestamp)
    ) as recompute_from_hour,
    md.resolved_on_timestamp
  from {{ ref('polymarket_polygon_market_details') }} md
  where md.token_id is not null
    and md.outcome in ('yes', 'no', '50/50')
),

-- de-duplicate metadata state to one row per token for deterministic merge keys
ranked_market_details as (
  select
    token_id,
    recompute_from_hour,
    resolved_on_timestamp,
    row_number() over (
      partition by token_id
      order by resolved_on_timestamp desc nulls last, recompute_from_hour desc
    ) as rn
  from market_details
),

state as (
  select
    token_id,
    recompute_from_hour,
    resolved_on_timestamp
  from ranked_market_details
  where rn = 1
),

{% if is_incremental() -%}
-- emit only tokens whose recompute-relevant state changed since the last helper snapshot
filtered as (
  select
    s.token_id,
    s.recompute_from_hour,
    case
      when prev.token_id is null then current_timestamp
      when coalesce(s.recompute_from_hour, timestamp '1970-01-01 00:00:00') <> coalesce(prev.recompute_from_hour, timestamp '1970-01-01 00:00:00') then current_timestamp
      when s.resolved_on_timestamp is not null
        and s.resolved_on_timestamp > coalesce(prev.change_detected_at, timestamp '1970-01-01 00:00:00')
        then s.resolved_on_timestamp
      else coalesce(s.resolved_on_timestamp, s.recompute_from_hour)
    end as change_detected_at
  from state s
  left join {{ this }} prev
    on prev.token_id = s.token_id
  where prev.token_id is null
     or coalesce(s.recompute_from_hour, timestamp '1970-01-01 00:00:00') <> coalesce(prev.recompute_from_hour, timestamp '1970-01-01 00:00:00')
     or (
       s.resolved_on_timestamp is not null
       and s.resolved_on_timestamp > coalesce(prev.change_detected_at, timestamp '1970-01-01 00:00:00')
     )
)
{% else -%}
-- on full-refresh, persist baseline state without marking all tokens as freshly changed
filtered as (
  select
    s.token_id,
    s.recompute_from_hour,
    coalesce(s.resolved_on_timestamp, s.recompute_from_hour) as change_detected_at
  from state s
)
{% endif -%}

select
  token_id,
  recompute_from_hour,
  change_detected_at
from filtered
where change_detected_at is not null
