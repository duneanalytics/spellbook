{{ config(
  materialized = 'table',
  schema = 'relay',
  alias = 'relay_token_transfers_all'
) }}

SELECT * FROM {{ ref('relay_token_transfers_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_optimism') }}
UNION ALL
SELECT * FROM {{ ref('relay_token_transfers_polygon') }}
