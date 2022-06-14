 {{
  config(
        alias='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_id'
  )
}}

SELECT 
  signatures[0] || id as unique_trade_id,
  'solana' as blockchain,
  signatures[0] as tx_hash, 
  block_time,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price AS amount_usd,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) AS amount,
  p.symbol as token_symbol,
  p.contract_address as token_address,
  account_keys[0] as traders
FROM {{ source('solana','transactions') }}
LEFT JOIN {{ source('prices', 'usd') }} p 
  ON p.minute = date_trunc('minute', block_time)
  AND p.symbol = 'SOL'        
WHERE (array_contains(account_keys, 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8') -- magic eden v1
       OR array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))  -- magic eden v2
AND ARRAY_CONTAINS(log_messages, 'Program log: Instruction: ExecuteSale')
AND block_time > '2021-09-01'