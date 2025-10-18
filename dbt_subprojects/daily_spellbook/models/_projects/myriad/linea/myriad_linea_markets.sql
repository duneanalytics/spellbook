{{
  config(
    schema = 'myriad_linea',
    alias = 'markets',
    materialized = 'view'
  )
}}

SELECT
'linea' AS blockchain,
call_block_time AS block_time,
call_block_number AS block_number,
call_tx_hash AS tx_hash,
call_tx_index AS tx_index,
call_tx_from AS market_creator_or_updater, 
contract_address,
output_marketId AS marketId,
json_extract_scalar(json_parse(desc), '$.token') as collateral_token,
json_extract_scalar(json_parse(desc), '$.question') as question,
(
    (
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(desc), '$.buyFees')), '$.fee') AS double) +
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(desc), '$.buyFees')), '$.treasuryFee') AS double) +
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(desc), '$.buyFees')), '$.distributorFee') AS double)
    )/1E18
) AS buy_fee,
(
    (
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(desc), '$.sellFees')), '$.fee') AS double) +
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(desc), '$.sellFees')), '$.treasuryFee') AS double) +
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(desc), '$.sellFees')), '$.distributorFee') AS double)
    )/1E18
) AS sell_fee,
false as points -- no points token on Linea (Only 0x176211869ca2b568f2a7d4ee941e073a821ee1ff is available)
FROM {{ source('myriad_linea', 'predictionmarketv3_4_call_createmarket') }}
WHERE call_success = TRUE