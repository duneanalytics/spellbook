 {{
  config(
        alias='trades')
}}

SELECT
      blockchain,
      project,
      version,
      block_time,
      explode(token_id) as token_id,
      collection,
      amount_usd/cardinality(token_id) as amount_usd,
      token_standard,
      trade_type,
      number_of_items/cardinality(token_id),
      trade_category,
      evt_type,
      seller,
      buyer,
      amount_original/cardinality(token_id) as amount_original,
      amount_raw/cardinality(token_id) as amount_raw,
      currency_symbol,
      currency_contract,
      nft_contract_address,
      project_contract_address,
      aggregator_name,
      aggregator_address,
      tx_hash,
      block_number,
      tx_from,
      tx_to,
      concat(token_id::string, '-', unique_trade_id) as unique_trade_id
FROM ({{ ref('sudoswap_ethereum_events') }})
WHERE evt_type = 'Trade'