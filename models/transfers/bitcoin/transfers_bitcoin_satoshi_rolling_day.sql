{{ config(
        schema = 'transfers_bitcoin',
        alias = 'satoshi_rolling_day',
        
)}}

select
    'bitcoin' as blockchain,
    day,
    wallet_address,
    NOW() as last_updated,
    row_number() over (partition by wallet_address order by day desc) as recency_index,
    sum(amount_raw) over (
        partition by wallet_address order by day
    ) as amount_raw,
    sum(amount_transfer_usd) over (
        partition by wallet_address order by day
    ) as amount_transfer_usd
from {{ ref('transfers_bitcoin_satoshi_agg_day') }}