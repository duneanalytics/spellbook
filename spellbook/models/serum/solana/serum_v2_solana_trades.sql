 {{
  config(schema = 'serum_v2_solana', alias='trades'
  )
}}

SELECT 
  signatures[0] || id || index::string as unique_trade_id,
  'solana' as blockchain,
  'serum' as project, 
  'v2' as version,
  signatures[0] as tx_hash, 
  block_time,
  srmprs.token_a_symbol,
  srmprs.token_b_symbol,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price AS amount_usd,
  account_keys[0] as trader_a,
  cast(NULL as string) AS trader_b,
  'EUqojwWA2rd19FZrzeBncJsm38Jm1hEhE3zsmX3bRc2o' as exchange_contract_address
FROM {{ source('solana','transactions') }}
LEFT JOIN {{ ref('serum_solana_pairs') }} srmprs ON array_contains(account_keys,srmprs.account_key)
LEFT JOIN {{ source('prices', 'usd') }} p 
  ON p.minute = date_trunc('minute', block_time)
  AND p.symbol = 'SOL'
WHERE (array_contains(account_keys, 'EUqojwWA2rd19FZrzeBncJsm38Jm1hEhE3zsmX3bRc2o'))
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND block_time > now() - interval 2 days
{% endif %}