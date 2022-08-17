{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}

SELECT 
  'solana' as blockchain,
  'magiceden' as project,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) THEN 'v2'
  WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN 'launchpad_v3'
  END as version,
  signatures[0] as tx_hash, 
  block_date,
  block_time,
  block_slot::string as block_number,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price AS amount_usd,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) AS amount_original,
  abs(post_balances[0] - pre_balances[0])::string AS amount_raw,
  p.symbol as currency_symbol,
  p.contract_address as currency_contract,
  'metaplex' as token_standard,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) THEN 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
       WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'
       END as project_contract_address,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) 
       AND array_contains(log_messages, 'Program log: Instruction: ExecuteSale')
       AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN 'Trade'
  WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) 
       AND array_contains(log_messages, 'Program log: Instruction: Sell') THEN 'List' 
  WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) 
       AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN 'Bid'
  WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) 
       AND array_contains(log_messages, 'Program log: Instruction: CancelBuy') THEN 'Cancel Bid'
  WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) 
       AND array_contains(log_messages, 'Program log: Instruction: CancelSell') THEN 'Cancel Listing'
  WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) 
       AND array_contains(log_messages, 'Program log: Instruction: SetAuthority') THEN 'Mint'
  ELSE 'Other' END as evt_type,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) 
         AND array_contains(log_messages, 'Program log: Instruction: ExecuteSale')
         AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN instructions[1].account_arguments[2]::string
       WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) 
         AND array_contains(log_messages, 'Program log: Instruction: SetAuthority') THEN COALESCE(instructions[6].account_arguments[9], instructions[5].account_arguments[9], 
         instructions[4].account_arguments[9], instructions[2].account_arguments[7], instructions[1].account_arguments[10], instructions[0].account_arguments[10])::string
       END AS token_id,
  NULL::string as collection,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) 
         AND array_contains(log_messages, 'Program log: Instruction: ExecuteSale') 
         AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN 'Single Item Trade' ELSE NULL::string 
         END as trade_type,
  '1' as number_of_items,
  NULL::string as trade_category,
  signer as buyer,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) 
         AND array_contains(log_messages, 'Program log: Instruction: ExecuteSale') 
         AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN instructions[2].account_arguments[1]::string
       WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN '' END as seller,
  NULL::string as nft_contract_address,
  NULL::string as aggregator_name,
  NULL::string as aggregator_address,
  NULL::string as tx_from,
  NULL::string as tx_to,
  2*(abs(post_balances[0] - pre_balances[0])::string)/100 as platform_fee_amount_raw,
  2*(abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9))/100 as platform_fee_amount,
  2*(abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price)/100 as platform_fee_amount_usd,
  '2' as platform_fee_percentage,
  abs(post_balances[11] - pre_balances[11]) + abs(post_balances[12] - pre_balances[12])
    + abs(post_balances[13] - pre_balances[13]) + abs(post_balances[14] - pre_balances[14])  + abs(post_balances[15] - pre_balances[15]) as royalty_fee_amount_raw,
  abs(post_balances[11] / 1e9 - pre_balances[11] / 1e9) + abs(post_balances[12] / 1e9 - pre_balances[12] / 1e9)
    + abs(post_balances[13] / 1e9 - pre_balances[13] / 1e9) + abs(post_balances[14] / 1e9 - pre_balances[14] / 1e9) + abs(post_balances[15] / 1e9 - pre_balances[15] / 1e9) 
    as royalty_fee_amount,
  (abs(post_balances[11] / 1e9 - pre_balances[11] / 1e9) + abs(post_balances[12] / 1e9 - pre_balances[12] / 1e9)
    + abs(post_balances[13] / 1e9 - pre_balances[13] / 1e9) + abs(post_balances[14] / 1e9 - pre_balances[14] / 1e9) + abs(post_balances[15] / 1e9 - pre_balances[15] / 1e9)) * 
    p.price as royalty_fee_amount_usd,
  ROUND(((abs(post_balances[10] / 1e9 - pre_balances[10] / 1e9)
  +abs(post_balances[11] / 1e9 - pre_balances[11] / 1e9)
  +abs(post_balances[12] / 1e9 - pre_balances[12] / 1e9)
  +abs(post_balances[13] / 1e9 - pre_balances[13] / 1e9)
  +abs(post_balances[14] / 1e9 - pre_balances[14] / 1e9)
  +abs(post_balances[15] / 1e9 - pre_balances[15] / 1e9)) / ((abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9)-0.00204928)) * 100),2) as royalty_fee_percentage,
  NULL::double as royalty_fee_receive_address,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) 
         AND array_contains(log_messages, 'Program log: Instruction: ExecuteSale') 
         AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN 'SOL' 
         ELSE NULL::string END as royalty_fee_currency_symbol,
  signatures[0] || '-' || id || '-' || instructions[0]::string as unique_trade_id,
  instructions, 
  signatures,
  log_messages
FROM {{ source('solana','transactions') }}
LEFT JOIN prices.usd p 
  ON p.minute = date_trunc('minute', block_time)
  AND p.symbol = 'SOL'        
WHERE (
     array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K') -- magic eden v2
     OR array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')
     )
     AND success = 'True'
     {% if not is_incremental() %}
     AND block_date > '2022-01-05'
     AND block_slot > 114980355
     {% endif %} 
     {% if is_incremental() %}
     -- this filter will only be applied on an incremental run
     AND block_date >= (select max(block_date) from {{ this }})
     {% endif %}