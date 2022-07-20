{{ config(
        alias ='events',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}

{% set v1_key = "MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8" %}
{% set v2_key = "M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K" %}
{% set launchpad_v1_key = "CMX5tvuWs2rBUL3vqVWiARfcDoCKjdeSinCsZdxJmYoF" %}
{% set launchpad_v2_key = "CMY8R8yghKfFnHKCWjzrArUpYH4PbJ56aWBr4kCP4DMk" %}
{% set launchpad_v3_key = "CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb" %}

SELECT 
  'solana' as blockchain,
  'magiceden' as project,
  CASE WHEN (array_contains(account_keys, '{{v1_key}}')) THEN 'v1'
  WHEN (array_contains(account_keys, '{{v2_key}}')) THEN 'v2'
  WHEN (array_contains(account_keys, '{{launchpad_v1_key}}')) THEN 'launchpad_v1'
  WHEN (array_contains(account_keys, '{{launchpad_v2_key}}')) THEN 'launchpad_v2'
  WHEN (array_contains(account_keys, '{{launchpad_v3_key}}')) THEN 'launchpad_v3'
  END as version,
  signatures[0] as tx_hash, 
  block_time,
  block_slot::string as block_number,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price AS amount_usd,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) AS amount_original,
  abs(post_balances[0] - pre_balances[0])::string AS amount_raw,
  p.symbol as currency_symbol,
  p.contract_address as currency_contract,
  'metaplex' as token_standard,
  CASE WHEN (array_contains(account_keys, '{{v1_key}}')) THEN '{{v1_key}}'
  WHEN (array_contains(account_keys, '{{v2_key}}')) THEN '{{v2_key}}'
  END as project_contract_address,
  CASE WHEN (array_contains(account_keys, '{{v1_key}}'))
  OR (array_contains(account_keys, '{{v2_key}}')) THEN 'Trade'
  WHEN (array_contains(account_keys, '{{launchpad_v1_key}}'))
  OR (array_contains(account_keys, '{{launchpad_v2_key}}'))
  OR (array_contains(account_keys, '{{launchpad_v3_key}}')) THEN 'Mint'
  ELSE 'Transaction' END as evt_type,
  signatures[0] || '-' || id || '-' || instructions[0]::string as unique_trade_id
FROM {{ source('solana','transactions') }}
LEFT JOIN {{ source('prices', 'usd') }} p 
  ON p.minute = date_trunc('minute', block_time)
  AND p.symbol = 'SOL'        
WHERE (
array_contains(account_keys, '{{v1_key}}')  -- magic eden v2
OR array_contains(account_keys, '{{v2_key}}')
OR array_contains(account_keys, '{{launchpad_v1_key}}')
OR array_contains(account_keys, '{{launchpad_v2_key}}')
OR array_contains(account_keys, '{{launchpad_v3_key}}'))
AND block_date > '2022-01-07'
AND block_slot > 114980355

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND block_date > now() - interval 2 days
{% endif %} 