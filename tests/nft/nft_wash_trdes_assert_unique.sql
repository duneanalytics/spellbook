-- Check for multiple unique_trade_id

select unique_trade_id
, COUNT(*)
from {{ ref('nft_wash_trades') }}
WHERE block_time >= NOW() - interval '2 days'
GROUP BY unique_trade_id
having COUNT(*) > 1