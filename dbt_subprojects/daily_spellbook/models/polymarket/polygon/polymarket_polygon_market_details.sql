{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_details',
    materialized = 'table',
    file_format = 'delta',
    full_refresh = true,
    post_hook='{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago", "0xboxer"]\') }}'
  )
}}

WITH onchain_metadata AS (
  SELECT
    evt_block_time AS block_time,
    evt_block_number AS block_number,
    marketId AS market_id,
    CASE
      WHEN json_value(from_utf8(data), 'lax $.title' NULL ON ERROR) IS NOT NULL
      THEN json_extract_scalar(from_utf8(data), '$.title')
      ELSE regexp_extract(from_utf8(data), 'title:\s*(.*?),\s*description:', 1)
    END AS neg_risk_market_name,
    CASE
      WHEN json_value(from_utf8(data), 'lax $.description' NULL ON ERROR) IS NOT NULL
      THEN json_extract_scalar(from_utf8(data), '$.description')
      ELSE substr(from_utf8(data), strpos(from_utf8(data), 'description: ') + length('description: '))
    END AS neg_risk_market_description,
    oracle,
    feeBips AS fee_bips,
    evt_index,
    evt_tx_hash AS tx_hash
  FROM {{ source('polymarket_polygon', 'NegRiskAdapter_evt_MarketPrepared') }}
)

,polymarket_api_upload as
(
    SELECT
      CASE 
        WHEN neg_risk = true THEN neg_risk_market_id
        WHEN neg_risk = false THEN cast(condition_id AS varchar)
      END AS unique_key,
      try_cast(substring(token_1_id, 5) AS UINT256) AS token_id,
      token_1_outcome AS token_outcome,
      *
    FROM {{ source('dune', 'dataset_polymarket_markets', database="dune") }}
    UNION ALL 
    SELECT
      CASE 
        WHEN neg_risk = true THEN neg_risk_market_id
        WHEN neg_risk = false THEN cast(condition_id AS varchar)
      END AS unique_key,
      try_cast(substring(token_2_id, 5) AS UINT256) AS token_id,
      token_2_outcome AS token_outcome,
      *
    FROM {{ source('dune', 'dataset_polymarket_markets', database="dune") }}
),

combine as 
(
Select 
    api.*, 
    oc.* 
from polymarket_api_upload api
LEFT JOIN onchain_metadata oc ON cast(oc.market_id AS varchar) = api.unique_key
)

, naming_things as (
SELECT 
  unique_key AS unique_key,
  condition_id AS condition_id,
  CASE 
    WHEN neg_risk_market_id IS NULL THEN NULL
    ELSE neg_risk_market_id
  END AS event_market_id,
  CASE 
    WHEN neg_risk_market_name IS NULL THEN 'single market'
    ELSE neg_risk_market_name
  END AS event_market_name,
  neg_risk_market_description as event_market_description,
  c.question_id AS question_id,
  question,
  description as market_description,
  token_id AS token_id,
  token_outcome,
  token_outcome || '-' || question as token_outcome_name,
  active,
  archived,
  closed,
  accepting_orders,
  enable_order_book,
  neg_risk,
  CASE 
    WHEN neg_risk = false THEN get_href('https://polymarket.com/event/' || market_slug, market_slug)
    WHEN neg_risk = true  THEN  get_href('https://polymarket.com/event/' || replace(replace(replace(lower(neg_risk_market_name), ' ', '-'), '$', ''), '''',''), neg_risk_market_name)
  END AS polymarket_link,
  accepting_order_timestamp as market_start_time,
  end_date_iso as market_end_time,
  game_start_time,
  seconds_delay,
  fpmm,
  icon,
  image,
  tags, 
  oracle AS oracle,
  fee_bips,
  case when pm.outcome is null then 'unresolved' else pm.outcome end as outcome,
  pm.block_time as resolved_on_timestamp,
  last_updated_at as last_uploaded_at
FROM combine c
left join {{ ref('polymarket_polygon_market_outcomes') }} pm on pm.question_id = c.question_id
)

SELECT * FROM naming_things