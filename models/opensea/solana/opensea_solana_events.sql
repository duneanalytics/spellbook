{{ config(
    schema = 'opensea_solana',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_trade_id']
    )
}}
With solana_events as (

SELECT
  'solana' as blockchain,
  'opensea' as project,
  'v1' as version,
  from_base58(signatures[1]) as tx_hash,
  block_time,
  CAST(block_slot AS bigint) as block_number,
  abs(post_balances[1] / 1e9 - pre_balances[1] / 1e9) * p.price AS amount_usd,
  abs(post_balances[1] / 1e9 - pre_balances[1] / 1e9) AS amount_original,
  CAST(abs(post_balances[1] - pre_balances[1]) AS uint256) AS amount_raw,
  p.symbol as currency_symbol,
  from_base58(p.contract_address)as currency_contract,
  'metaplex' as token_standard,
  from_base58(CASE WHEN (contains(account_keys, '3o9d13qUvEuuauhFrVom1vuCzgNsJifeaBYDPquaT73Y')) THEN '3o9d13qUvEuuauhFrVom1vuCzgNsJifeaBYDPquaT73Y'
  WHEN (contains(account_keys, 'pAHAKoTJsAAe2ZcvTZUxoYzuygVAFAmbYmJYdWT886r')) THEN 'pAHAKoTJsAAe2ZcvTZUxoYzuygVAFAmbYmJYdWT886r'
  END) as project_contract_address,
  'Trade' as evt_type,
  signatures[1] || '-' || id as unique_trade_id
FROM {{ source('solana','transactions') }}
LEFT JOIN {{ source('prices', 'usd') }} p
  ON p.minute = date_trunc('minute', block_time)
  AND p.blockchain is null
  AND p.symbol = 'SOL'
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
  {% endif %}
WHERE (contains(account_keys, '3o9d13qUvEuuauhFrVom1vuCzgNsJifeaBYDPquaT73Y')
       OR contains(account_keys, 'pAHAKoTJsAAe2ZcvTZUxoYzuygVAFAmbYmJYdWT886r'))
{% if not is_incremental() %}
AND block_time > TIMESTAMP '2022-04-06'
AND block_slot > 128251864
{% endif %}
{% if is_incremental() %}
AND block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
)

SELECT
    blockchain,
    project,
    version,
    block_time,
    CAST(NULL AS uint256) as token_id,
    CAST(NULL AS varchar) as collection,
    amount_usd,
    token_standard,
    CAST(NULL AS varchar) as trade_type,
    CAST(NULL AS uint256) as number_of_items,
    CAST(NULL AS varchar) as trade_category,
    evt_type,
    CAST(NULL AS varbinary) as seller,
    CAST(NULL AS varbinary) as buyer,
    amount_original,
    CAST(amount_raw AS uint256) AS amount_raw,
    currency_symbol,
    currency_contract,
    CAST(NULL AS varbinary) as nft_contract_address,
    project_contract_address,
    CAST(NULL AS varchar) as aggregator_name,
    CAST(NULL AS varbinary) as aggregator_address,
    tx_hash,
    block_number,
    CAST(NULL AS varbinary) as tx_from,
    CAST(NULL AS varbinary) as tx_to,
    CAST(NULL AS uint256) AS platform_fee_amount_raw,
    CAST(NULL AS DOUBLE) AS platform_fee_amount,
    CAST(NULL AS DOUBLE) AS platform_fee_amount_usd,
    CAST(NULL AS DOUBLE) as platform_fee_percentage,
    CAST(NULL AS uint256) as royalty_fee_amount_raw,
    CAST(NULL AS DOUBLE) as royalty_fee_amount,
    CAST(NULL AS DOUBLE) as royalty_fee_amount_usd,
    CAST(NULL AS DOUBLE) as royalty_fee_percentage,
    CAST(NULL AS varbinary) as royalty_fee_receive_address,
    CAST(NULL AS varchar) as royalty_fee_currency_symbol,
    unique_trade_id
FROM solana_events
