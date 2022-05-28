 {{
  config(
    alias='active_traders_day'
  )
}}

SELECT 
  'ethereum' as blockchain, 
  date as day, 
  count(account) AS traders
FROM
  (SELECT date_trunc('day', block_time) AS date,
          maker AS account
   FROM {{ ref('opensea_ethereum_trades') }}
   UNION SELECT date_trunc('day', block_time) AS date,
                taker AS account
   FROM {{ ref('opensea_ethereum_trades') }}) a
GROUP BY 2
ORDER BY 2