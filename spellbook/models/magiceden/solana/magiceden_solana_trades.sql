 {{
  config(
        alias='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge'
  )
}}

SELECT 
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
WHERE (array_contains(account_keys, 'rFqFJ9g7TGBD8Ed7TPDnvGKZ5pWLPDyxLcvcH2eRCtt')
       OR array_contains(account_keys, '2NZukH2TXpcuZP4htiuT8CFxcaQSWzkkR6kepSWnZ24Q'))
AND block_time > '2021-09-01'
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND block_time > (select max(block_time) from {{ this }})
{% endif %} 