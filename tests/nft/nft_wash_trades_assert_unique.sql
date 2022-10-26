-- Check for multiple unique_trade_id

SELECT unique_trade_id
, COUNT(*)
FROM {{ ref('nft_wash_trades') }}
WHERE block_time >= NOW() - interval '2 days'
GROUP BY unique_trade_id
HAVING COUNT(*) > 1