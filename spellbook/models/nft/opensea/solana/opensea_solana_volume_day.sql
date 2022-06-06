 {{
  config(
    alias='volume_day'
  )
}}

SELECT 
  'solana' as blockchain,
  date_trunc('day', block_time) as day,
  sum(amount) AS volume,
  'SOL' as token_symbol,
  sum(amount_usd) AS volume_usd
FROM {{ ref('opensea_solana_trades') }}
GROUP BY 2