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

WITH compute_until AS (
  SELECT date_trunc('hour', MAX(block_time)) AS blocker
  FROM {{ source('tokens_polygon', 'transfers') }}
  )

, polymarket_first_funded AS (
  SELECT u.polymarket_wallet AS address
  , ftr.block_number AS first_token_received_block
  FROM {{ ref('polymarket_polygon_users') }} u
  INNER JOIN {{ source('addresses_events_polygon', 'first_token_received')}} ftr ON u.polymarket_wallet = ftr.address
  )

-- get the last computed hour in the spell
{% if is_incremental() %}
, last_spell_update AS (
  SELECT MAX(block_hour) AS max_hour
  FROM {{this}}
  )
{% endif %}

, relevant_transfers_in AS (
  SELECT date_trunc('hour', t.block_time) AS block_hour
  , t.block_date
  , t.block_month
  , to_user.address AS polymarket_wallet
  , SUM(t.amount) AS amount
  FROM {{ source('tokens_polygon', 'transfers') }} t
  INNER JOIN polymarket_first_funded to_user ON t."to" = to_user.address
    AND t.block_number >= to_user.first_token_received_block
  WHERE t.contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
  AND t.block_number >= 5067840
  AND t.block_time < (SELECT blocker FROM compute_until)
  {% if is_incremental() %}
  AND {{ incremental_predicate('t.block_date') }}
  AND t.block_time > (SELECT max_hour FROM last_spell_update)
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
    AND t.block_number >= from_user.first_token_received_block
  WHERE t.contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
  AND t.block_number >= 5067840
  AND t.block_time < (SELECT blocker FROM compute_until)
  {% if is_incremental() %}
  AND {{ incremental_predicate('t.block_date') }}
  AND t.block_time > (SELECT max_hour FROM last_spell_update)
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

{% if is_incremental() %}

, wallets_to_compute AS (
  SELECT DISTINCT polymarket_wallet
  FROM all_relevant_transfers
  )

, backfilled_balances AS (
  SELECT polymarket_wallet
  , COALESCE(SUM(amount), 0) AS backfilled_amount
  FROM {{this}}
  LEFT JOIN wallets_to_compute USING (polymarket_wallet)
  )

{% endif %}

, final_balances AS (
  SELECT block_hour
  , block_date
  , block_month
  , polymarket_wallet
  , SUM(amount) AS holding_change
  , SUM(SUM(amount)) OVER (PARTITION BY polymarket_wallet ORDER BY block_hour) AS amount_held
  FROM all_relevant_transfers
  GROUP BY 1, 2, 3, 4
  )

{% if is_incremental() %}

SELECT block_hour
, block_date
, block_month
, polymarket_wallet
, holding_change
, amount_held+backfilled_amount AS amount_held
FROM final_balances
INNER JOIN backfilled_balances USING (polymarket_wallet)

{% else %}

SELECT *
FROM final_balances

{% endif %}