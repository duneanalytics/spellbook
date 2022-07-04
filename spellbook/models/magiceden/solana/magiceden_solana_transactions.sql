{{ config(
        alias ='transactions',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}

SELECT 
  'solana' as blockchain,
  'magiceden' as project,
  CASE WHEN (array_contains(account_keys, 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8')) THEN 'v1'
  WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) THEN 'v2' 
  WHEN (array_contains(account_keys, 'CMX5tvuWs2rBUL3vqVWiARfcDoCKjdeSinCsZdxJmYoF')) THEN 'launchpad_v1'
  WHEN (array_contains(account_keys, 'CMY8R8yghKfFnHKCWjzrArUpYH4PbJ56aWBr4kCP4DMk')) THEN 'launchpad_v2'
  WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN 'launchpad_v3'
  END as version,
  signatures[0] as tx_hash, 
  block_time,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price AS amount_usd,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) AS amount_original,
  abs(post_balances[0] - pre_balances[0])::string AS amount_raw,
  p.symbol as currency_symbol,
  p.contract_address as currency_contract,
  'metaplex' as token_standard,
  CASE WHEN (array_contains(account_keys, 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8')) THEN 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8'
  WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) THEN 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' 
  END as project_contract_address,
  CASE WHEN (array_contains(account_keys, 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8')) 
  OR (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) THEN 'Trade' 
  WHEN (array_contains(account_keys, 'CMX5tvuWs2rBUL3vqVWiARfcDoCKjdeSinCsZdxJmYoF'))
  OR (array_contains(account_keys, 'CMY8R8yghKfFnHKCWjzrArUpYH4PbJ56aWBr4kCP4DMk'))
  OR (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN 'Mint'
  ELSE 'Transaction' END as evt_type,
  signatures[0] || '-' || id as unique_trade_id
FROM {{ source('solana','transactions') }}
LEFT JOIN {{ source('prices', 'usd') }} p 
  ON p.minute = date_trunc('minute', block_time)
  AND p.symbol = 'SOL'        
WHERE (array_contains(account_keys, 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8'))  -- magic eden v2
OR (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
OR (array_contains(account_keys, 'CMX5tvuWs2rBUL3vqVWiARfcDoCKjdeSinCsZdxJmYoF'))
OR (array_contains(account_keys, 'CMY8R8yghKfFnHKCWjzrArUpYH4PbJ56aWBr4kCP4DMk'))
OR (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'))
AND block_date > '2022-01-07'

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND block_date > now() - interval 2 days
{% endif %} 