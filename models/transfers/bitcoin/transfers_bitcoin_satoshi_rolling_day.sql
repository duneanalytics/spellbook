{{ config(
        schema = 'transfers_bitcoin',
        alias = alias('satoshi_rolling_day'),
        tags = ['dunesql']
)}}

select
    'bitcoin' as blockchain,
    day,
    wallet_address,
    NOW() as last_updated,
    row_number() over (partition by wallet_address order by day desc) as recency_index,
    sum(amount_raw) over (
        partition by wallet_address order by day
    ) as amount_raw
from {{ ref('transfers_bitcoin_satoshi_agg_day') }}