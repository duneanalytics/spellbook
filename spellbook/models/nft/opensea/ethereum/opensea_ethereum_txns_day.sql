 {{
  config(
    alias='txns_day'
  )
}}

SELECT 
  'ethereum' as blockchain,
  date_trunc('day', block_time) as day,
  count(DISTINCT tx_hash) as transactions
FROM {{ ref('opensea_ethereum_trades') }}
GROUP BY 2 
ORDER BY 2