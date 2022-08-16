 {{
  config(
        alias='fees')
}}

SELECT blockchain,
project,
version,
block_time,
token_id,
NULL::string as collection,
platform_fee_amount_raw,
platform_fee_amount,
platform_fee_amount_usd,
platform_fee_percentage,
royalty_fee_amount_raw,
royalty_fee_amount,
royalty_fee_amount_usd,
royalty_fee_percentage,
NULL::string as royalty_fee_receive_address,
royalty_fee_currency_symbol,
token_standard,
trade_type,
number_of_items,
NULL::string as trade_category,
evt_type,
seller,
buyer,
NULL::string as nft_contract_address,
project_contract_address,
NULL::string as aggregator_name,
NULL::string as aggregator_address,
block_number,
tx_hash,
NULL::string as tx_from,
NULL::string as tx_to,
unique_trade_id
FROM {{ ref('magiceden_solana_events') }}