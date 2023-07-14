{{ config(
	tags=['legacy'],
	
        alias = alias('bep20_rolling_day', legacy_model=True)
        )
}}

select
    'bnb' as blockchain,
    day,
    wallet_address,
    token_address,
    symbol,
    NOW() as last_updated,
    row_number() over (partition by token_address, wallet_address order by day desc) as recency_index,
    sum(amount_raw) over (
        partition by token_address, wallet_address order by day
    ) as amount_raw,
    sum(amount) over (
        partition by token_address, wallet_address order by day
    ) as amount
from {{ ref('transfers_bnb_bep20_agg_day_legacy') }}
;