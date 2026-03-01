{{
  config(
    schema = 'polymarket_polygon',
    alias = 'users_balance_changes',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'block_hour', 'polymarket_wallet'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["hildobby"]\') }}'
  )
}}

WITH polymarket_first_funded AS (
  SELECT u.polymarket_wallet AS address
  , ffb.block_number AS first_funded_block
  FROM {{ ref('polymarket_polygon_users') }} u
  LEFT JOIN {{ source('addresses_events_polygon', 'first_funded_by')}} ffb ON u.polymarket_wallet = ffb.address
)

, relevant_transfers_in AS (
  SELECT date_trunc('hour', t.block_time) AS block_hour
  , t.block_date
  , t.block_month
  , to_user.address AS polymarket_wallet
  , SUM(t.amount) AS amount
  FROM {{ source('tokens_polygon', 'transfers') }} t
  INNER JOIN polymarket_first_funded to_user ON t."to" = to_user.address
  WHERE t.contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
  AND t.block_number >= 5067840
  {% if is_incremental() %}
  AND {{ incremental_predicate('t.block_date') }}
  {% endif %}
  GROUP BY 1, 2, 3, 4
  )

, relevant_transfers_out AS (
  SELECT date_trunc('hour', t.block_time) AS block_hour
  , t.block_date
  , t.block_month
  , from_user.address AS polymarket_wallet
  , -SUM(t.amount) AS amount
  FROM {{ source('tokens_polygon', 'transfers') }} t
  INNER JOIN polymarket_first_funded from_user ON t."from" = from_user.address
  WHERE t.contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
  AND t.block_number >= 5067840
  {% if is_incremental() %}
  AND {{ incremental_predicate('t.block_date') }}
  {% endif %}
  GROUP BY 1, 2, 3, 4
)

, all_relevant_transfers AS (
    SELECT block_hour
  , block_date
  , block_month
  , polymarket_wallet
  , amount
  FROM relevant_transfers_in

  UNION ALL

  SELECT block_hour
  , block_date
  , block_month
  , polymarket_wallet
  , amount
  FROM relevant_transfers_out
  )

SELECT block_hour
, block_date
, block_month
, polymarket_wallet
, SUM(amount) AS holding_change
, SUM(SUM(amount)) OVER (PARTITION BY polymarket_wallet ORDER BY block_hour) AS amount_held
FROM all_relevant_transfers
GROUP BY 1, 2, 3, 4