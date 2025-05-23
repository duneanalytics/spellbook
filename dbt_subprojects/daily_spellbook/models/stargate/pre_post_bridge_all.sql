{{ config(
  materialized='table',
  schema='bridge_user_tracking',
  alias='pre_post_bridge_all'
) }}

SELECT * FROM {{ ref('pre_post_bridge_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_arbitrum') }}
UNION ALL
SELECT * FROM {{ ref('pre_post_bridge_polygon') }}
