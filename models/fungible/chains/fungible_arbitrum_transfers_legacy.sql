{{ config(
    schema = 'fungible_arbitrum',
    alias = alias('transfers', legacy_model=True),
    tags=['legacy']
)}}

SELECT NULL AS blockchain
, NULL AS block_time
, NULL AS block_number
, NULL AS amount_raw
, NULL AS amount
, NULL AS usd_price
, NULL AS usd_amount
, NULL AS contract_address
, NULL AS symbol
, NULL AS decimals
, 'native' AS token_standard
, NULL AS tx_from
, NULL AS from
, NULL AS to
, NULL AS tx_hash
, NULL AS evt_index