{{ config(
        schema = 'magiceden_polygon',
        alias ='trades'
        )
}}

SELECT
  blockchain,
  project,
  version,
  tx_hash,
  block_date,
  block_time,
  block_number,
  amount_usd,
  amount_original,
  amount_raw,
  currency_symbol,
  currency_contract,
  token_id,
  token_standard,
  project_contract_address,
  evt_type,
  collection,
  trade_type,
  number_of_items,
  trade_category,
  buyer,
  seller,
  nft_contract_address,
  aggregator_name,
  aggregator_address,
  tx_from,
  tx_to,
  platform_fee_amount_raw,
  platform_fee_amount,
  platform_fee_amount_usd,
  platform_fee_percentage,
  royalty_fee_amount_raw,
  royalty_fee_amount,
  royalty_fee_amount_usd,
  royalty_fee_percentage,
  royalty_fee_receive_address,
  royalty_fee_currency_symbol,
  unique_trade_id
FROM {{ ref('magiceden_polygon_events') }}
WHERE evt_type = 'Trade'
