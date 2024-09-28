{{
  config(
    schema = 'compound_v1_ethereum',
    alias = 'base_borrow',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transaction_type', 'token_address', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

SELECT 'ethereum' AS blockchain
, 'compound' AS project
, '1' AS version
, 'borrow' AS transaction_type
, 'variable' AS loan_type
, asset AS token_address
, account AS borrower
, account AS on_behalf_of
, CAST(NULL AS varbinary) AS repayer
, CAST(NULL AS varbinary) AS liquidator
, amount
, CAST(date_trunc('month', evt_block_time) AS date) AS block_month
, evt_block_time AS block_time
, evt_block_number AS block_number
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index
FROM {{ source('compound_v1_ethereum', 'MoneyMarket_evt_BorrowTaken') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}

UNION ALL

SELECT 'ethereum' AS blockchain
, 'compound' AS project
, '1' AS version
, 'repay' AS transaction_type
, 'variable' AS loan_type
, asset AS token_address
, account AS borrower
, account AS on_behalf_of
, account AS repayer
, CAST(NULL AS varbinary) AS liquidator
, amount
, CAST(date_trunc('month', evt_block_time) AS date) AS block_month
, evt_block_time AS block_time
, evt_block_number AS block_number
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index
FROM {{ source('compound_v1_ethereum', 'MoneyMarket_evt_BorrowRepaid') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}

UNION ALL

SELECT 'ethereum' AS blockchain
, 'compound' AS project
, '1' AS version
, 'borrow_liquidation' AS transaction_type
, 'variable' AS loan_type
, assetBorrow AS token_address
, targetAccount AS borrower
, targetAccount AS on_behalf_of
, CAST(NULL AS varbinary) AS repayer
, liquidator
, amountRepaid AS amount
, CAST(date_trunc('month', evt_block_time) AS date) AS block_month
, evt_block_time AS block_time
, evt_block_number AS block_number
, contract_address AS project_contract_address
, evt_tx_hash AS tx_hash
, evt_index
FROM {{ source('compound_v1_ethereum', 'MoneyMarket_evt_BorrowLiquidated') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}