{{ config(
        alias ='bep20_rolling_month'
        )
}}

WITH transfers_bnb_bep20_agg_day_with_month AS (
    SELECT day,
           wallet_address,
           token_address,
           symbol,
           amount_raw,
           amount,
           date_trunc('month', day) AS month
    FROM {{ ref('transfers_bnb_bep20_agg_day') }}
),

transfers_bnb_bep20_agg_month AS (
    SELECT month,
           wallet_address,
           token_address,
           symbol,
           sum(amount_raw) AS amount_raw,
           sum(amount) AS amount
    FROM transfers_bnb_bep20_agg_day_with_month
    GROUP BY month, wallet_address, token_address, symbol
)

SELECT
    'bnb' as blockchain,
    month,
    wallet_address,
    token_address,
    symbol,
    current_timestamp() as last_updated,
    row_number() over (partition by token_address, wallet_address order by month desc) as recency_index,
    sum(amount_raw) over (
        partition by token_address, wallet_address order by month
    ) as amount_raw,
    sum(amount) over (
        partition by token_address, wallet_address order by month
    ) as amount
FROM transfers_bnb_bep20_agg_month
;