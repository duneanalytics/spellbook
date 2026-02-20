{{
  config(
    schema = 'myriad_abstract',
    alias = 'markets',
    materialized = 'view'
  )
}}

SELECT
'abstract' AS blockchain,
call_block_time AS block_time,
call_block_number AS block_number,
call_tx_hash AS tx_hash,
call_tx_index AS tx_index,
call_tx_from AS market_creator_or_updater, 
contract_address,
output_0 AS marketId,
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
CASE WHEN LOWER(json_extract_scalar(json_parse(desc), '$.token')) = LOWER('0x0b07cf011B6e2b7E0803b892d97f751659940F23') THEN true ELSE false END as points
FROM {{ source('myriad_abstract', 'predictionmarketv3_3_points_call_createmarket') }}
WHERE call_success = TRUE

UNION DISTINCT

SELECT
'abstract' AS blockchain,
call_block_time AS block_time,
call_block_number AS block_number,
call_tx_hash AS tx_hash,
call_tx_index AS tx_index,
call_tx_from AS market_creator_or_updater, 
contract_address,
marketId,
json_extract_scalar(json_parse(update), '$.token') as collateral_token,
json_extract_scalar(json_parse(update), '$.question') as question,
(
    (
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(update), '$.buyFees')), '$.fee') AS double) +
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(update), '$.buyFees')), '$.treasuryFee') AS double) +
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(update), '$.buyFees')), '$.distributorFee') AS double)
    )/1E18
) AS buy_fee,
(
    (
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(update), '$.sellFees')), '$.fee') AS double) +
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(update), '$.sellFees')), '$.treasuryFee') AS double) +
    CAST(json_extract_scalar(json_parse(json_extract_scalar(json_parse(update), '$.sellFees')), '$.distributorFee') AS double)
    )/1E18
) AS sell_fee,
CASE WHEN LOWER(json_extract_scalar(json_parse(update), '$.token')) = LOWER('0x0b07cf011B6e2b7E0803b892d97f751659940F23') THEN true ELSE false END as points
FROM {{ source('myriad_abstract', 'predictionmarketv3_3_points_call_updatemarket') }} -- markets can be updated so let's also fetch updates
WHERE call_success = TRUE

UNION DISTINCT
  
SELECT
'abstract' AS blockchain,
call_block_time AS block_time,
call_block_number AS block_number,
call_tx_hash AS tx_hash,
call_tx_index AS tx_index,
call_tx_from AS market_creator_or_updater, 
contract_address,
output_0 AS marketId,
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
CASE WHEN LOWER(json_extract_scalar(json_parse(desc), '$.token')) = LOWER('0x0b07cf011B6e2b7E0803b892d97f751659940F23') THEN true ELSE false END as points
FROM {{ source('myriad_abstract', 'predictionmarketv4_call_createmarket') }}
WHERE call_success = TRUE

UNION DISTINCT
  
SELECT
'abstract' AS blockchain,
call_block_time AS block_time,
call_block_number AS block_number,
call_tx_hash AS tx_hash,
call_tx_index AS tx_index,
call_tx_from AS market_creator_or_updater, 
contract_address,
output_0 AS marketId,
json_extract_scalar(json_parse(desc), '$.token') as collateral_token,
json_extract_scalar(json_parse(desc), '$.question') as question,
(CAST(json_extract_scalar(json_parse(desc), '$.fee') AS double) +
CAST(json_extract_scalar(json_parse(desc), '$.treasuryFee') AS double)) as buy_fee,
(CAST(json_extract_scalar(json_parse(desc), '$.fee') AS double) +
CAST(json_extract_scalar(json_parse(desc), '$.treasuryFee') AS double)) as sell_fee, -- Believe the fee is taken on both sides in this version
CASE WHEN LOWER(json_extract_scalar(json_parse(desc), '$.token')) = LOWER('0x0b07cf011B6e2b7E0803b892d97f751659940F23') THEN true ELSE false END as points
FROM {{ source('myriad_abstract', 'predictionmarketv3_call_createmarket') }}
WHERE call_success = TRUE
AND LOWER(json_extract_scalar(json_parse(desc), '$.token')) IN (LOWER('0x84a71ccd554cc1b02749b35d22f684cc8ec987e1'), LOWER('0x9ebe3a824ca958e4b3da772d2065518f009cba62'), LOWER('0xf19609e96187cdaa34cffb96473fac567e547302')) -- tokens + points token
-- Test tokens: 0xe0638f4de0dc948e131a00b30c26acf5d912d4ad, 0xf716edec846f854aab117ca9a5efd930972736dc, 0x11589630b42d751ff69e93e15b812cff336de0f9, 0x91d8ef5b5cbb562f2064664a1eda0484557886aa, 0x99c013b4c36062efb4fd41c7ab68218da9e873d4, 0xac1b0e5b574ab073f073d9ec77a756cb980698de

