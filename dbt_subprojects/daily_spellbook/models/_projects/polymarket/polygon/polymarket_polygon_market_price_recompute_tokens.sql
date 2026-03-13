{{ config(
    schema = 'polymarket_polygon',
    alias = 'market_price_recompute_tokens',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['token_id']
  )
}}

-- collect token-level recompute boundaries and latest relevant metadata change timestamps
with market_details as (
  select
    md.token_id,
    cast(date_trunc('hour', try(from_iso8601_timestamp(md.market_end_time))) as timestamp) as recompute_from_hour,
    coalesce(
      md.resolved_on_timestamp,
      try_cast(md.last_uploaded_at as timestamp)
    ) as change_detected_at
  from {{ ref('polymarket_polygon_market_details') }} md
  where md.token_id is not null
    and md.outcome in ('yes', 'no')
    and try(from_iso8601_timestamp(md.market_end_time)) is not null
),

-- keep only rows relevant for this incremental run based on recent change detection
filtered as (
  select
    token_id,
    recompute_from_hour,
    change_detected_at
  from market_details
  {% if is_incremental() -%}
  where change_detected_at >= coalesce(
    (select date_add('day', -2, max(change_detected_at)) from {{ this }}),
    timestamp '1970-01-01 00:00:00'
  )
  {% endif -%}
),

-- pick the most recent change record per token for deterministic merge keys
ranked as (
  select
    token_id,
    recompute_from_hour,
    change_detected_at,
    row_number() over (partition by token_id order by change_detected_at desc) as rn
  from filtered
)

select
  token_id,
  recompute_from_hour,
  change_detected_at
from ranked
where rn = 1
