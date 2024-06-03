{{ config(
    schema = 'magiceden_solana',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_trade_id']
    )
}}

WITH me_txs AS (
    SELECT
    id,
    pre_balances,
    post_balances,
    block_slot,
    block_time,
    account_keys,
    log_messages,
    instructions,
    signatures,
    signer,
    filter(
        instructions,
        x -> (
            x.executing_account = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
            OR x.executing_account = 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'
        )
    ) AS me_instructions
    FROM {{ source('solana','transactions') }}
    WHERE (
         contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K') -- magic eden v2
         OR contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')
    )
    AND success = true
    {% if not is_incremental() %}
    AND block_time > TIMESTAMP '2022-01-05'
    AND block_slot > 114980355
    {% endif %}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    AND block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

)
SELECT
  'solana' as blockchain,
  'magiceden' as project,
  CASE WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) THEN 'v2'
  WHEN (contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN 'launchpad_v3'
  END as version,
  from_base58(signatures[1]) as tx_hash,
  block_time,
  CAST(block_slot AS BIGINT) as block_number,
  abs(element_at(post_balances,1) / 1e9 - element_at(pre_balances,1) / 1e9) * p.price AS amount_usd,
  abs(element_at(post_balances,1) / 1e9 - element_at(pre_balances,1) / 1e9) AS amount_original,
  CAST(abs(element_at(post_balances,1) - element_at(pre_balances,1)) AS uint256) AS amount_raw,
  p.symbol as currency_symbol,
  from_base58(p.contract_address) as currency_contract,
  'metaplex' as token_standard,
  CASE WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) THEN from_base58('M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')
       WHEN (contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN from_base58('CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')
       END as project_contract_address,
  CASE WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
       AND (
               contains(log_messages, 'Program log: Instruction: ExecuteSaleV2')
               OR contains(log_messages, 'Program log: Instruction: ExecuteSale')
               OR contains(log_messages, 'Program log: Instruction: Mip1ExecuteSaleV2')
          )
       AND contains(log_messages, 'Program log: Instruction: Buy') THEN 'Trade'
  WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
       AND contains(log_messages, 'Program log: Instruction: Sell') THEN 'List'
  WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
       AND contains(log_messages, 'Program log: Instruction: Buy') THEN 'Bid'
  WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
       AND contains(log_messages, 'Program log: Instruction: CancelBuy') THEN 'Cancel Bid'
  WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
       AND contains(log_messages, 'Program log: Instruction: CancelSell') THEN 'Cancel Listing'
  WHEN (contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'))
       AND contains(log_messages, 'Program log: Instruction: SetAuthority') THEN 'Mint'
  ELSE 'Other' END as evt_type,
  Coalesce(TRY_CAST(CASE WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
         AND (
               contains(log_messages, 'Program log: Instruction: ExecuteSaleV2')
               OR contains(log_messages, 'Program log: Instruction: ExecuteSale')
               OR contains(log_messages, 'Program log: Instruction: Mip1ExecuteSaleV2')
          )
         AND contains(log_messages, 'Program log: Instruction: Buy') THEN element_at(element_at(me_instructions,2).account_arguments,3)
       WHEN (contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'))
         AND contains(log_messages, 'Program log: Instruction: SetAuthority') THEN COALESCE(element_at(me_instructions,7).account_arguments[10], element_at(me_instructions,6).account_arguments[10],
         element_at(me_instructions,5).account_arguments[10], element_at(element_at(me_instructions,3).account_arguments,8), element_at(element_at(me_instructions,2).account_arguments,11), element_at(element_at(me_instructions,1).account_arguments,11))
       END as uint256), UINT256 '0') AS token_id,
  cast(NULL as varchar) as collection,
  CASE WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
         AND (
               contains(log_messages, 'Program log: Instruction: ExecuteSaleV2')
               OR contains(log_messages, 'Program log: Instruction: ExecuteSale')
               OR contains(log_messages, 'Program log: Instruction: Mip1ExecuteSaleV2')
          )
         AND contains(log_messages, 'Program log: Instruction: Buy') THEN 'Single Item Trade' ELSE NULL
         END as trade_type,
  uint256 '1' as number_of_items,
  cast(NULL as varchar) as trade_category,
  from_base58(signer) as buyer,
  CASE WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
         AND (
               contains(log_messages, 'Program log: Instruction: ExecuteSaleV2')
               OR contains(log_messages, 'Program log: Instruction: ExecuteSale')
               OR contains(log_messages, 'Program log: Instruction: Mip1ExecuteSaleV2')
          )
         AND contains(log_messages, 'Program log: Instruction: Buy') THEN from_base58(element_at(element_at(me_instructions,3).account_arguments,2))
       WHEN (contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN cast(null as varbinary) END as seller,
  cast(NULL as varbinary) as nft_contract_address,
  cast(NULL as varchar) as aggregator_name,
  cast(NULL as varbinary) as aggregator_address,
  cast(NULL as varbinary) as tx_from,
  cast(NULL as varbinary) as tx_to,
  cast(2*(abs(element_at(post_balances,1) - element_at(pre_balances,1)))/100 as uint256) as platform_fee_amount_raw,
  2*(abs(element_at(post_balances,1) / 1e9 - element_at(pre_balances,1) / 1e9))/100 as platform_fee_amount,
  2*(abs(element_at(post_balances,1) / 1e9 - element_at(pre_balances,1) / 1e9) * p.price)/100 as platform_fee_amount_usd,
  DOUBLE '2' as platform_fee_percentage,
  CAST (abs(element_at(post_balances,12) - element_at(pre_balances,12)) + abs(element_at(post_balances,13) - element_at(pre_balances,13))
    + abs(element_at(post_balances,14) - element_at(pre_balances,14)) + abs(element_at(post_balances,15) - element_at(pre_balances,15))  + abs(element_at(post_balances,16) - element_at(pre_balances,16)) AS uint256) as royalty_fee_amount_raw,
  abs(element_at(post_balances,12) / 1e9 - element_at(pre_balances,12) / 1e9) + abs(element_at(post_balances,13) / 1e9 - element_at(pre_balances,13) / 1e9)
    + abs(element_at(post_balances,14) / 1e9 - element_at(pre_balances,14) / 1e9) + abs(element_at(post_balances,15) / 1e9 - element_at(pre_balances,15) / 1e9) + abs(element_at(post_balances,16) / 1e9 - element_at(pre_balances,16) / 1e9)
    as royalty_fee_amount,
  (abs(element_at(post_balances,12) / 1e9 - element_at(pre_balances,12) / 1e9) + abs(element_at(post_balances,13) / 1e9 - element_at(pre_balances,13) / 1e9)
    + abs(element_at(post_balances,14) / 1e9 - element_at(pre_balances,14) / 1e9) + abs(element_at(post_balances,15) / 1e9 - element_at(pre_balances,15) / 1e9) + abs(element_at(post_balances,16) / 1e9 - element_at(pre_balances,16) / 1e9)) *
    p.price as royalty_fee_amount_usd,
  ROUND(((abs(element_at(post_balances,11) / 1e9 - element_at(pre_balances,11) / 1e9)
  +abs(element_at(post_balances,12) / 1e9 - element_at(pre_balances,12) / 1e9)
  +abs(element_at(post_balances,13) / 1e9 - element_at(pre_balances,13) / 1e9)
  +abs(element_at(post_balances,14) / 1e9 - element_at(pre_balances,14) / 1e9)
  +abs(element_at(post_balances,15) / 1e9 - element_at(pre_balances,15) / 1e9)
  +abs(element_at(post_balances,16) / 1e9 - element_at(pre_balances,16) / 1e9)) / ((abs(element_at(post_balances,1) / 1e9 - element_at(pre_balances,1) / 1e9)-0.00204928)) * 100),2) as royalty_fee_percentage,
  cast(NULL as varbinary) as royalty_fee_receive_address,
  CASE WHEN (contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
         AND (
               contains(log_messages, 'Program log: Instruction: ExecuteSaleV2')
               OR contains(log_messages, 'Program log: Instruction: ExecuteSale')
               OR contains(log_messages, 'Program log: Instruction: Mip1ExecuteSaleV2')
          )
          AND contains(log_messages, 'Program log: Instruction: Buy') THEN 'SOL'
         ELSE cast(NULL as varchar) END as royalty_fee_currency_symbol,
  id  as unique_trade_id,
  instructions,
  signatures,
  log_messages,
  BIGINT '0' as evt_index
FROM me_txs
LEFT JOIN {{ source('prices', 'usd') }} AS p
  ON p.minute = date_trunc('minute', block_time)
  AND p.blockchain is NULL
  AND p.symbol = 'SOL'
  {% if is_incremental() %}
  AND p.minute >= date_trunc('day', now() - interval '7' day)
  {% endif %}
