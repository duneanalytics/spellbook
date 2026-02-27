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

WITH polymarket_first_funded AS (
  SELECT u.polymarket_wallet AS address
  , ffb.block_number AS first_funded_block
  FROM  {{ ref('polymarket_polygon_users') }} u
  LEFT JOIN {{ source('addresses_events_polygon', 'first_funded_by')}} ffb ON u.polymarket_wallet = ffb.address
)

, relevant_transfers_in AS (
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
  FROM {{ source('tokens_polygon', 'transfers') }} t
  INNER JOIN {{ ref('polymarket_polygon_users') }} to_user ON t."to" = to_user.polymarket_wallet
  WHERE t.contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
  AND t.block_number >= 5067840
  {% if is_incremental() %}
  AND {{ incremental_predicate('t.block_time') }}
  {% endif %}
  )

, relevant_transfers_out AS (
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
  , from_user.polymarket_wallet AS from_polymarket_wallet
  FROM {{ source('tokens_polygon', 'transfers') }} t
  INNER JOIN {{ ref('polymarket_polygon_users') }} from_user ON t."from" = from_user.polymarket_wallet
  WHERE t.contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
  AND t.block_number >= 5067840
  {% if is_incremental() %}
  AND {{ incremental_predicate('t.block_time') }}
  {% endif %}
)

, relevant_transfers_all AS (
  SELECT block_time
  , block_date
  , block_month
  , block_number
  , tx_hash
  , tx_from
  , tx_to
  , tx_index
  , evt_index
  , amount
  , from_address
  , to_address
  , unique_key
  , CAST(NULL AS VARBINARY) AS from_polymarket_wallet
  , to_polymarket_wallet
  FROM relevant_transfers_in

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
  , amount
  , from_address
  , to_address
  , unique_key
  , from_polymarket_wallet
  , CAST(NULL AS VARBINARY) AS to_polymarket_wallet
  FROM relevant_transfers_in
  )

, relevant_transfers AS (
  SELECT MAX(block_time) AS block_time
  , MAX(block_date) AS block_date
  , MAX(block_month) AS block_month
  , MAX(block_number) AS block_number
  , MAX(tx_hash) AS tx_hash
  , MAX(tx_from) AS tx_from
  , MAX(tx_to) AS tx_to
  , MAX(tx_index) AS tx_index
  , MAX(evt_index) AS evt_index
  , MAX(amount) AS amount
  , MAX(from_address) AS from_address
  , MAX(to_address) AS to_address
  , unique_key
  , MAX(from_polymarket_wallet) AS from_polymarket_wallet
  , MAX(to_polymarket_wallet) AS to_polymarket_wallet
  FROM relevant_transfers_all
  GROUP BY unique_key
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
  FROM relevant_transfers_with_ffb
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
  FROM relevant_transfers_with_ffb
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
  FROM relevant_transfers_with_ffb
  WHERE from_polymarket_wallet IS NOT NULL AND to_polymarket_wallet IS NOT NULL
  AND (from_first_funded_block IS NULL OR from_first_funded_block <= block_number)
) t