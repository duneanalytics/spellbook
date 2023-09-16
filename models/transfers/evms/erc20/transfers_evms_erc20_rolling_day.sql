{{ config(
        schema = 'transfers_evms',
        alias = alias('erc20_rolling_day'),
        partition_by = ['blockchain', 'day'],
        tags = ['dunesql']
)}}

select
    blockchain,
    day,
    wallet_address,
    token_address,
    symbol,
    NOW() as last_updated,
    row_number() over (partition by wallet_address order by day desc) as recency_index,
    sum(amount_raw) over (
        partition by blockchain, token_address, wallet_address order by day
    ) as amount_raw,
    sum(amount) over (
        partition by blockchain, token_address, wallet_address order by day
    ) as amount,
    sum(amount_transfer_usd) over (
        partition by blockchain, token_address, wallet_address order by day
    ) as amount_transfer_usd
from {{ ref('transfers_evms_erc20_agg_day') }}