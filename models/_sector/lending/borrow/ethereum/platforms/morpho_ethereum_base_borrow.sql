{{
  config(
    schema = 'morpho_ethereum',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

WITH markets AS (
    SELECT id
    , from_hex(JSON_EXTRACT_SCALAR("marketParams", '$.loanToken')) AS loanToken
    , from_hex(JSON_EXTRACT_SCALAR("marketParams", '$.collateralToken')) AS collateralToken
    , from_hex(JSON_EXTRACT_SCALAR("marketParams", '$.oracle')) AS oracle
    , JSON_EXTRACT_SCALAR("marketParams", '$.irm') AS irm
    , JSON_EXTRACT_SCALAR("marketParams", '$.lltv') AS lltv
    FROM {{ source('morpho_blue_ethereum', 'MorphoBlue_evt_CreateMarket') }}
    )

, base_borrow AS (
  SELECT 'borrow' AS transaction_type
  , 'variable' AS loan_type
  , loanToken AS token_address
  , caller AS borrower
  , onBehalf AS on_behalf_of
  , CAST(NULL AS varbinary) AS repayer
  , CAST(NULL AS varbinary) AS liquidator
  , CAST(assets AS double) AS amount
  , contract_address
  , evt_tx_hash
  , evt_index
  , evt_block_time
  , evt_block_number
  FROM {{ source('morpho_blue_ethereum', 'MorphoBlue_evt_Borrow') }}
  INNER JOIN markets USING (id)
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}

  UNION ALL

  SELECT 'repay' AS transaction_type
  , NULL AS loan_type
  , loanToken AS token_address
  , caller AS borrower
  , onBehalf AS on_behalf_of
  , caller AS repayer
  , cast(null as varbinary) AS liquidator
  , -1 * cast(assets AS double) AS amount
  , contract_address
  , evt_tx_hash
  , evt_index
  , evt_block_time
  , evt_block_number
  FROM {{ source('morpho_blue_ethereum', 'MorphoBlue_evt_Repay') }}
  INNER JOIN markets USING (id)
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}

  UNION ALL

  select 'borrow_liquidation' AS transaction_type
  , null AS loan_type
  , loanToken AS token_address
  , borrower
  , borrower AS on_behalf_of
  , caller AS repayer
  , caller AS liquidator
  , -1 * cast(repaidAssets AS double) AS amount
  , contract_address
  , evt_tx_hash
  , evt_index
  , evt_block_time
  , evt_block_number
  FROM {{ source('morpho_blue_ethereum', 'MorphoBlue_evt_Liquidate') }}
  INNER JOIN markets USING (id)
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}
  )

SELECT 'ethereum' AS blockchain
, 'morpho' AS project
, '1' AS version
, transaction_type
, loan_type
, token_address
, borrower
, on_behalf_of
, repayer
, liquidator
, amount
, CAST(date_trunc('month', evt_block_time) AS date) AS block_month
, evt_block_time AS block_time
, evt_block_number AS block_number
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index
FROM base_borrow