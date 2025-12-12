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

SELECT t.block_time
, t.block_date
, t.block_month
, t.block_number
, t.tx_hash
, t.tx_from
, t.tx_to
, t.tx_index
, t.evt_index
, 'inflow' AS direction
, a.polymarket_wallet
, t.amount
, "from"
, CAST(NULL AS varbinary) AS to
, t.unique_key
FROM {{ source('tokens_polygon', 'transfers') }} t
INNER JOIN {{ ref('polymarket_polygon_users') }} a ON t.to=a.polymarket_wallet
INNER JOIN {{ source('addresses_events_polygon', 'first_funded_by')}} ffb ON t.to=ffb.address
  AND ffb.block_number<=t.block_number
WHERE t.contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
AND t.block_number >= 5067840
{% if is_incremental() %}
AND {{ incremental_predicate('t.block_time') }}
{% endif %}

UNION ALL

SELECT t.block_time
, t.block_date
, t.block_month
, t.block_number
, t.tx_hash
, t.tx_from
, t.tx_to
, t.tx_index
, t.evt_index
, 'outflow' AS direction
, a.polymarket_wallet
, t.amount
, CAST(NULL AS varbinary) AS "from"
, to
, t.unique_key
FROM {{ source('tokens_polygon', 'transfers') }} t
INNER JOIN {{ ref('polymarket_polygon_users') }} a ON t."from"=a.polymarket_wallet
INNER JOIN {{ source('addresses_events_polygon', 'first_funded_by')}} ffb ON t."from"=ffb.address
  AND ffb.block_number<=t.block_number
WHERE contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
AND t.block_number >= 5067840
{% if is_incremental() %}
AND {{ incremental_predicate('t.block_time') }}
{% endif %}