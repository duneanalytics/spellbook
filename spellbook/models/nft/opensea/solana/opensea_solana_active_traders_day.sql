 {{
  config(
    alias='active_traders_day'
  )
}}

SELECT 
  'solana' as blockchain, 
    date_trunc('day', block_time) as day,
    count(DISTINCT traders) AS traders
FROM  {{ ref('opensea_solana_trades') }}
GROUP BY 2