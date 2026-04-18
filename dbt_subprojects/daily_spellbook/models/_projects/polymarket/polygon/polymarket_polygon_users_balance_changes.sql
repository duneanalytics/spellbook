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
  FROM {{ ref('polymarket_polygon_users_capital_actions') }}
  )

, polymarket_wallets AS (
  SELECT polymarket_wallet
  FROM {{ ref('polymarket_polygon_users_address_lookup') }}
  )

-- get the last computed hour in the spell
{% if is_incremental() %}
, last_spell_update AS (
  SELECT MAX(block_hour) AS max_hour
  FROM {{ this }}
  )
{% endif %}

, relevant_transfers_in AS (
  SELECT date_trunc('hour', ca.block_time) AS block_hour
  , ca.block_date
  , cast(date_trunc('month', ca.block_time) as date) AS block_month
  , wallet.polymarket_wallet
  , SUM(ca.amount) AS amount
  FROM {{ ref('polymarket_polygon_users_capital_actions') }} ca
  INNER JOIN polymarket_wallets wallet ON ca.to_address = wallet.polymarket_wallet
  WHERE ca.block_time < (SELECT blocker FROM compute_until)
  {% if is_incremental() %}
  AND {{ incremental_predicate('ca.block_date') }}
  AND ca.block_time > (SELECT max_hour FROM last_spell_update)
  {% endif %}
  GROUP BY 1, 2, 3, 4
  )

, relevant_transfers_out AS (
  SELECT date_trunc('hour', ca.block_time) AS block_hour
  , ca.block_date
  , cast(date_trunc('month', ca.block_time) as date) AS block_month
  , wallet.polymarket_wallet
  , -SUM(ca.amount) AS amount
  FROM {{ ref('polymarket_polygon_users_capital_actions') }} ca
  INNER JOIN polymarket_wallets wallet ON ca.from_address = wallet.polymarket_wallet
  WHERE ca.block_time < (SELECT blocker FROM compute_until)
  {% if is_incremental() %}
  AND {{ incremental_predicate('ca.block_date') }}
  AND ca.block_time > (SELECT max_hour FROM last_spell_update)
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
  , COALESCE(MAX_BY(amount_held, block_hour), 0) AS backfilled_amount
  FROM wallets_to_compute
  LEFT JOIN {{this}} USING (polymarket_wallet)
  GROUP BY 1
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
, amount_held + backfilled_amount AS amount_held
FROM final_balances
INNER JOIN backfilled_balances USING (polymarket_wallet)

{% else %}

SELECT *
FROM final_balances

{% endif %}