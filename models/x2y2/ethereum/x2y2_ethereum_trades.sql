 {{
  config(
        alias='trades',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "x2y2",
                                    \'["hildobby","soispoke"]\') }}')
}}

SELECT blockchain,
project,
version,
block_time,
CAST(token_id AS VARCHAR(100))AS token_id,
collection,
amount_usd,
token_standard,
trade_type,
CAST(number_of_items AS DECIMAL(38,0)) AS number_of_items,
trade_category,
evt_type,
seller,
buyer,
amount_original,
CAST(amount_raw AS DECIMAL(38,0)) AS amount_raw,
currency_symbol,
currency_contract,
nft_contract_address,
project_contract_address,
aggregator_name,
aggregator_address,
block_number,
tx_hash,
tx_from,
tx_to,
unique_trade_id
FROM {{ ref('x2y2_ethereum_events') }}
WHERE evt_type = 'Trade'