{{
  config(
    schema = 'polymarket_polygon',
    alias = 'users_balance_changes',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_index', 'evt_index', 'direction'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

WITH relevant_transfers AS (
  SELECT t.block_time
  , t.block_date
  , t.block_month
  , t.block_number
  , t.tx_hash
  , t.tx_from
  , t.tx_to
  , t.tx_index
  , t.evt_index
  , t.amount
  , t."from" AS from_address
  , t."to" AS to_address
  , t.unique_key
  , to_user.polymarket_wallet AS to_polymarket_wallet
  , from_user.polymarket_wallet AS from_polymarket_wallet
  , to_ffb.first_funded_block_number AS to_first_funded_block
  , from_ffb.first_funded_block_number AS from_first_funded_block
  FROM {{ source('tokens_polygon', 'transfers') }} t
  LEFT JOIN {{ ref('polymarket_polygon_users') }} to_user ON t."to" = to_user.polymarket_wallet
  LEFT JOIN {{ ref('polymarket_polygon_users') }} from_user ON t."from" = from_user.polymarket_wallet
  INNER JOIN {{ source('addresses_events_polygon', 'first_funded_by')}} to_ffb ON t."to" = to_ffb.address
  INNER JOIN {{ source('addresses_events_polygon', 'first_funded_by')}} from_ffb ON t."from" = from_ffb.address
  WHERE t.contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
  AND t.block_number >= 5067840
  AND (to_user.polymarket_wallet IS NOT NULL OR from_user.polymarket_wallet IS NOT NULL)
  {% if is_incremental() %}
  AND {{ incremental_predicate('t.block_time') }}
  {% endif %}
  )
  
SELECT block_time
, block_date
, block_month
, block_number
, tx_hash
, tx_from
, tx_to
, tx_index
, evt_index
, direction
, polymarket_wallet
, amount
, "from"
, "to"
, unique_key
FROM (
  SELECT block_time
  , block_date
  , block_month
  , block_number
  , tx_hash
  , tx_from
  , tx_to
  , tx_index
  , evt_index
  , 'inflow' AS direction
  , to_polymarket_wallet AS polymarket_wallet
  , amount
  , from_address as "from"
  , cast(NULL AS varbinary) AS "to"
  , unique_key
  FROM relevant_transfers
  WHERE to_polymarket_wallet IS NOT NULL
    AND (to_first_funded_block IS NULL OR to_first_funded_block <= block_number)
  
  UNION ALL
  
  SELECT block_time
  , block_date
  , block_month
  , block_number
  , tx_hash
  , tx_from
  , tx_to
  , tx_index
  , evt_index
  , 'outflow' AS direction
  , from_polymarket_wallet AS polymarket_wallet
  , amount
  , cast(NULL AS varbinary) AS "from"
  , to_address AS "to"
  , unique_key
  FROM relevant_transfers
  WHERE from_polymarket_wallet IS NOT NULL
  AND (from_first_funded_block IS NULL OR from_first_funded_block <= block_number)
  
  UNION ALL
  
  SELECT block_time
  , block_date
  , block_month
  , block_number
  , tx_hash
  , tx_from
  , tx_to
  , tx_index
  , evt_index
  , 'internal' AS direction
  , from_polymarket_wallet AS polymarket_wallet
  , amount
  , cast(NULL AS varbinary) AS "from"
  , to_address AS "to"
  , unique_key
  FROM relevant_transfers
  WHERE from_polymarket_wallet IS NOT NULL AND to_polymarket_wallet IS NOT NULL
  AND (from_first_funded_block IS NULL OR from_first_funded_block <= block_number)
) t