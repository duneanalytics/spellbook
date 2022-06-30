 {{
  config(
        alias='fees')
}}

SELECT blockchain,
project,
version,
block_time,
token_id,
collection,
fee_amount_raw,
fee_amount,
fee_amount_usd,
fee_receive_address,
fee_currency_symbol,
token_standard,
trade_type,
number_of_items,
trade_category,
evt_type,
seller,
buyer,
nft_contract_address,
project_contract_address,
aggregator_name,
aggregator_address,
tx_hash,
tx_from,
tx_to,
unique_trade_id
FROM ({{ ref('opensea_ethereum_transactions') }})