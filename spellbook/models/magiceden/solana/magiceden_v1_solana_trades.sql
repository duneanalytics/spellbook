 {{
  config(schema = 'magiceden_v1_solana', 
        alias='trades'
  )
}}

SELECT 
  'solana' as blockchain,
  'magiceden' as project,
  'v1' as version,
  signatures[0] as tx_hash, 
  block_time,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price AS amount_usd,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) AS amount_original,
  abs(post_balances[0] - pre_balances[0])::string AS amount_raw,
  p.symbol as currency_symbol,
  p.contract_address as currency_contract,
  'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8' as project_contract_address,
  'ExecuteSale' as evt_type,
  account_keys[0] as traders,
  signatures[0] || '-' || id as unique_trade_id
FROM {{ source('solana','transactions') }}
LEFT JOIN {{ source('prices', 'usd') }} p 
  ON p.minute = date_trunc('minute', block_time)
  AND p.symbol = 'SOL'        
WHERE (array_contains(account_keys, 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8')) -- magic eden v1
AND ARRAY_CONTAINS(log_messages, 'Program log: Instruction: ExecuteSale')
AND block_date > '2021-09-01'