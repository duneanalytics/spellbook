{{ config(
        tags = ['dunesql'],
        alias = alias('matic_day'),
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}
WITH 

time_seq AS (
    SELECT 
        sequence(
        CAST('2020-05-30' as timestamp),
        date_trunc('day', cast(now() as timestamp)),
        interval '1' day
        ) AS time 
),

days AS (
    SELECT 
        time.time AS day 
    FROM time_seq
    CROSS JOIN unnest(time) AS time(time)
),

daily_balances as (
    SELECT
        blockchain, 
        day, 
        wallet_address, 
        token_address, 
        amount_raw,
        amount,
        symbol,
        LEAD(day, 1, current_timestamp) OVER (PARTITION BY token_address, wallet_address ORDER BY day) AS next_day
    FROM 
    {{ ref('transfers_polygon_matic_rolling_day') }}
)

SELECT
    b.blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    b.symbol
FROM 
daily_balances b
INNER JOIN 
days d 
    ON b.day <= d.day 
    AND d.day < b.next_day
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND d.day = p.minute
    AND p.blockchain = 'polygon'
