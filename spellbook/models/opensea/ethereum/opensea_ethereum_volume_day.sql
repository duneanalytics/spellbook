 {{
  config(
    alias='volume_day'
  )
}}

  SELECT
  'ethereum' as blockchain,
  date_trunc('day', block_time) AS day,
  SUM(amount) AS volume,
  token_symbol,
  SUM(amount_usd) AS volume_usd
FROM
  {{ ref('opensea_ethereum_trades') }}
GROUP BY 2,4
ORDER BY 2