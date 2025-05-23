{{ config(
  materialized='table',
  schema='stargate',
  alias='stargate_bridge_transfers_all'
) }}

SELECT * FROM {{ ref('stargate_bridge_transfers_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_arbitrum') }}
UNION ALL
SELECT * FROM {{ ref('stargate_bridge_transfers_polygon') }}
