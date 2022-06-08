 {{
  config(schema = 'serum_v3_solana',
        alias='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_id'
  )
}}

SELECT 
  signatures[0] || id || fee::string as unique_id,
  'solana' as blockchain,
  'serum' as project, 
  'v3' as version,
  signatures[0] as tx_hash, 
  block_time,
  srmprs.token_a_symbol,
  srmprs.token_b_symbol,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price AS amount_usd,
  account_keys[0] as trader_a,
  cast(NULL as string) AS trader_b,
  '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin' as exchange_contract_address,
  id as trade_id
FROM {{ source('solana','transactions') }}
LEFT JOIN {{ ref('serum_solana_pairs') }} srmprs ON array_contains(account_keys,srmprs.account_key)
LEFT JOIN {{ source('prices', 'usd') }} p 
  ON p.minute = date_trunc('minute', block_time)
  AND p.symbol = 'SOL'
WHERE (array_contains(account_keys, '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'))
AND block_time > '2021-02-16'
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND block_time > now() - interval 2 days
{% endif %}
