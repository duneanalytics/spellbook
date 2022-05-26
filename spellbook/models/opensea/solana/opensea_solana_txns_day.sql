 {{
  config(
    alias='txns_day'
    )
}}

SELECT 
       'solana' as blockchain,
       date_trunc('day', block_time) as day,
       count(DISTINCT tx_hash) as transactions
FROM {{ ref('opensea_solana_trades') }}
GROUP BY 2