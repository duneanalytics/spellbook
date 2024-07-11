{{
  config(
    schema = 'morpho_ethereum',
    alias = 'base_supply',
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

SELECT 'ethereum' AS blockchain
, 'morpho' AS project
, '1' AS version
, 'deposit' AS transaction_type
, loanToken AS token_address
, caller AS depositor
, onBehalf AS on_behalf_of
, CAST(NULL AS varbinary) AS withdrawn_to
, CAST(NULL AS varbinary) AS liquidator
, CAST(assets AS double) AS amount
, CAST(date_trunc('month', evt_block_time) AS date) AS block_month
, evt_block_time AS block_time
, evt_block_number AS block_number
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index
FROM {{ source('morpho_blue_ethereum', 'MorphoBlue_evt_Supply') }}
INNER JOIN markets USING (id)
{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}

UNION ALL

SELECT 'ethereum' AS blockchain
, 'morpho' AS project
, '1' AS version
, 'withdraw' AS transaction_type
, loanToken AS token_address
, CAST(NULL AS varbinary) AS depositor
, onBehalf AS on_behalf_of
, receiver AS withdrawn_to
, CAST(NULL AS varbinary) AS liquidator
, -1 * CAST(assets AS double) AS amount
, CAST(date_trunc('month', evt_block_time) AS date) AS block_month
, evt_block_time AS block_time
, evt_block_number AS block_number
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index
FROM {{ source('morpho_blue_ethereum', 'MorphoBlue_evt_Withdraw') }}
INNER JOIN markets USING (id)
{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}

UNION ALL

SELECT 'ethereum' AS blockchain
, 'morpho' AS project
, '1' AS version
, 'deposit_liquidation' AS transaction_type
, loanToken AS token_address
, borrower AS depositor
, CAST(NULL AS varbinary) AS on_behalf_of
, caller AS withdrawn_to
, caller AS liquidator
, -1 * CAST(seizedAssets AS double) AS amount
, CAST(date_trunc('month', evt_block_time) AS date) AS block_month
, evt_block_time AS block_time
, evt_block_number AS block_number
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index
FROM {{ source('morpho_blue_ethereum', 'MorphoBlue_evt_Liquidate') }}
INNER JOIN markets USING (id)
{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}