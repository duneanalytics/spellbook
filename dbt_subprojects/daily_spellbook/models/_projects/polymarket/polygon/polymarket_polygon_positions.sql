{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions',
    materialized = 'view',
    post_hook = '{{ private_data_explorer(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket") }}'
  )
}}

with enriched as (
  select
    p.day
    , p.address
    , mm.unique_key
    , p.token_id
    , mm.token_outcome
    , mm.token_outcome_name
    , p.balance
    , mm.question_id
    , mm.question as market_question
    , mm.market_description
    , mm.event_market_name
    , mm.event_market_description
    , mm.active
    , mm.closed
    , mm.accepting_orders
    , mm.polymarket_link
    , mm.market_start_time
    , mm.market_end_time
    , mm.outcome as market_outcome
    , mm.resolved_on_timestamp
    , cast(p.last_updated as timestamp(3) with time zone) as _position_updated_at
    , cast(
      case
        when mm.last_uploaded_at is null
          and mm.resolved_on_timestamp is null
        then null
        else greatest(
          coalesce(mm.last_uploaded_at, timestamp '1970-01-01 00:00:00 UTC')
          , coalesce(mm.resolved_on_timestamp, timestamp '1970-01-01 00:00:00 UTC')
        )
      end as timestamp(3) with time zone
    ) as _market_updated_at
  from {{ ref('polymarket_polygon_positions_raw') }} as p
  inner join {{ ref('polymarket_polygon_market_details') }} as mm
    on p.token_id = mm.token_id
)

select
  day
  , address
  , unique_key
  , token_id
  , token_outcome
  , token_outcome_name
  , balance
  , question_id
  , market_question
  , market_description
  , event_market_name
  , event_market_description
  , active
  , closed
  , accepting_orders
  , polymarket_link
  , market_start_time
  , market_end_time
  , market_outcome
  , resolved_on_timestamp
  , _position_updated_at
  , _market_updated_at
  , cast(
    greatest(
      _position_updated_at
      , coalesce(_market_updated_at, _position_updated_at)
    ) as timestamp(3) with time zone
  ) as _updated_at
from enriched
