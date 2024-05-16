{{ config(
    schema = 'element_linea',
    
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

WITH base_trades AS (
    SELECT 'linea' as blockchain,
  'element' as project,
  'v1' as project_version,
  evt_block_time AS block_time,
  cast(date_trunc('day', evt_block_time) as date) as block_date,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_number AS block_number,
  'Buy' AS trade_category,
  'secondary' AS trade_type,
  erc1155Token AS nft_contract_address,
  cast(erc1155TokenId as uint256) AS nft_token_id,
  uint256 '1' AS nft_amount,
  taker AS buyer,
  maker AS seller,
  cast(erc20FillAmount AS UINT256) AS price_raw,
  CASE
    WHEN erc20Token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee AND '{{blockchain}}' = 'zksync' THEN 0x000000000000000000000000000000000000800a
    WHEN erc20Token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x0000000000000000000000000000000000000000
    ELSE erc20Token
  END AS currency_contract,
  CAST(IF(CARDINALITY(fees) >= 1, JSON_EXTRACT_SCALAR(JSON_PARSE(fees[1]), '$.amount'), '0') AS UINT256) AS platform_fee_amount_raw,
  CAST(IF(CARDINALITY(fees) >= 2, JSON_EXTRACT_SCALAR(JSON_PARSE(fees[2]), '$.amount'), '0') AS UINT256) royalty_fee_amount_raw,
  from_hex(IF(CARDINALITY(fees) >= 1, JSON_EXTRACT_SCALAR(JSON_PARSE(fees[1]), '$.recipient'), NULL)) AS platform_fee_address,
  from_hex(IF(CARDINALITY(fees) >= 2, JSON_EXTRACT_SCALAR(JSON_PARSE(fees[2]), '$.recipient'), NULL)) AS royalty_fee_address,
  contract_address AS project_contract_address,
  evt_tx_hash AS tx_hash,
  evt_index AS sub_tx_trade_id
FROM {{ source('element_ex_linea','ERC1155OrdersFeature_evt_ERC1155BuyOrderFilled') }}
{% if is_incremental() %}
WHERE {{incremental_predicate('evt_block_time')}}
{% endif %}

UNION ALL

SELECT
  '{{blockchain}}' as blockchain,
  'element' as project,
  'v1' as project_version,
  evt_block_time AS block_time,
  cast(date_trunc('day', evt_block_time) as date) as block_date,
  cast(date_trunc('month', evt_block_time) as date) as block_month,
  evt_block_number AS block_number,
  'Buy' AS trade_category,
  'secondary' AS trade_type,
  erc1155Token AS nft_contract_address,
  cast(erc1155TokenId as uint256) AS nft_token_id,
  uint256 '1' AS nft_amount,
  maker AS buyer,
  taker AS seller,
  cast(erc20FillAmount AS UINT256) AS price_raw,
  CASE
    WHEN erc20Token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee AND '{{blockchain}}' = 'zksync' THEN 0x000000000000000000000000000000000000800a
    WHEN erc20Token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x0000000000000000000000000000000000000000
    ELSE erc20Token
  END AS currency_contract,
  CAST(IF(CARDINALITY(fees) >= 1, JSON_EXTRACT_SCALAR(JSON_PARSE(fees[1]), '$.amount'), '0') AS UINT256) AS platform_fee_amount_raw,
  CAST(IF(CARDINALITY(fees) >= 2, JSON_EXTRACT_SCALAR(JSON_PARSE(fees[2]), '$.amount'), '0') AS UINT256) royalty_fee_amount_raw,
  from_hex(IF(CARDINALITY(fees) >= 1, JSON_EXTRACT_SCALAR(JSON_PARSE(fees[1]), '$.recipient'), NULL)) AS platform_fee_address,
  from_hex(IF(CARDINALITY(fees) >= 2, JSON_EXTRACT_SCALAR(JSON_PARSE(fees[2]), '$.recipient'), NULL)) AS royalty_fee_address,
  contract_address AS project_contract_address,
  evt_tx_hash AS tx_hash,
  evt_index AS sub_tx_trade_id
FROM {{ source('element_ex_linea','ERC1155OrdersFeature_evt_ERC1155SellOrderFilled') }}
{% if is_incremental() %}
WHERE {{incremental_predicate('evt_block_time')}}
{% endif %}
)

{{ add_nft_tx_data('base_trades', 'linea') }}