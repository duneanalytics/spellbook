{{
  config(
    schema = 'polymarket_polygon',
    alias = 'position_metadata_recompute_tokens',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['token_id'],
    merge_skip_unchanged = true
  )
}}

with metadata_state as (
  select
    md.token_id,
    coalesce(
      cast(md.market_start_time_parsed as date),
      cast(md.resolved_on_timestamp as date),
      date '2020-09-03'
    ) as recompute_from_day,
    concat_ws(
      '||',
      coalesce(md.unique_key, ''),
      coalesce(md.token_outcome, ''),
      coalesce(md.token_outcome_name, ''),
      coalesce(cast(md.question_id as varchar), ''),
      coalesce(md.question, ''),
      coalesce(md.market_description, ''),
      coalesce(md.event_market_name, ''),
      coalesce(md.event_market_description, ''),
      coalesce(cast(md.active as varchar), ''),
      coalesce(cast(md.closed as varchar), ''),
      coalesce(cast(md.accepting_orders as varchar), ''),
      coalesce(md.polymarket_link, ''),
      coalesce(md.market_start_time, ''),
      coalesce(md.market_end_time, ''),
      coalesce(md.outcome, ''),
      coalesce(cast(md.resolved_on_timestamp as varchar), '')
    ) as metadata_state
  from {{ ref('polymarket_polygon_market_details') }} as md
  where md.token_id is not null
),

{% if is_incremental() %}
filtered as (
  select
    ms.token_id,
    ms.recompute_from_day,
    ms.metadata_state,
    current_timestamp as change_detected_at
  from metadata_state as ms
  left join {{ this }} as prev
    on prev.token_id = ms.token_id
  where prev.token_id is null
     or ms.recompute_from_day <> prev.recompute_from_day
     or ms.metadata_state <> prev.metadata_state
)
{% else %}
filtered as (
  select
    ms.token_id,
    ms.recompute_from_day,
    ms.metadata_state,
    cast(ms.recompute_from_day as timestamp) as change_detected_at
  from metadata_state as ms
)
{% endif %}

select
  token_id,
  recompute_from_day,
  metadata_state,
  change_detected_at
from filtered
