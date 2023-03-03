 {{
  config(
        alias='trades',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "archipelago",
                                    \'["0xRob"]\') }}')
}}

SELECT
      blockchain,
      project,
      version,
      block_time,
      token_id,
      collection,
      amount_usd,
      token_standard,
      trade_type,
      number_of_items,
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
      tx_hash,
      block_number,
      tx_from,
      tx_to,
      unique_trade_id
FROM {{ ref('archipelago_ethereum_events') }}
