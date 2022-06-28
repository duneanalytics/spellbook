 {{
  config(schema = 'magiceden_v2_solana', 
        alias='trades'
  )
}}

SELECT 
  'solana' as blockchain,
  'magiceden' as project,
  'v2' as version,
  signatures[0] as tx_hash, 
  block_time,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price AS amount_usd,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) AS amount_original,
  p.symbol as currency_symbol,
  p.contract_address as currency_contract,
  'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' as project_contract_address,
  account_keys[0] as traders,
  signatures[0] || '-' || id as unique_trade_id
FROM {{ source('solana','transactions') }}
LEFT JOIN {{ source('prices', 'usd') }} p 
  ON p.minute = date_trunc('minute', block_time)
  AND p.symbol = 'SOL'        
WHERE (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))  -- magic eden v2
AND ARRAY_CONTAINS(log_messages, 'Program log: Instruction: ExecuteSale')
AND block_date > '2022-01-07'