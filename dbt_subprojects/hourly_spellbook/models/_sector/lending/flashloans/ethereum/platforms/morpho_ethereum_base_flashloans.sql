{{
  config(
    schema = 'morpho_ethereum',
    alias = 'base_flashloans',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

SELECT 'ethereum' AS blockchain
, 'morpho' AS project
, '1' AS version
, caller AS recipient
, assets AS amount
, CAST(0 AS UINT256) AS fee
, token AS token_address
, contract_address AS project_contract_address
, date_trunc('month', evt_block_time) AS block_month
, evt_block_time AS block_time
, evt_block_number AS block_number
, evt_tx_hash AS tx_hash
, evt_index
FROM {{ source('morpho_blue_ethereum', 'MorphoBlue_evt_FlashLoan') }}
{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}
