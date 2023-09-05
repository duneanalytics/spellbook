{{ config(
        tags = ['dunesql'],
        alias = alias('hour'),
        post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

WITH 

time_seq AS (
    SELECT 
        sequence(
        CAST('2023-06-15' as timestamp),
        date_trunc('hour', cast(now() as timestamp)),
        interval '1' hour
        ) AS time 
),

hours AS (
    SELECT 
        time.time AS hour
    FROM time_seq
    CROSS JOIN unnest(time) AS time(time)
),

hourly_balances as (
    SELECT 
        blockchain, 
        hour, 
        wallet_address, 
        token_address, 
        amount_raw,
        amount,
        symbol,
        LEAD(hour, 1, current_timestamp) OVER (PARTITION BY token_address, wallet_address ORDER BY hour) AS next_hour
    FROM 
    {{ ref('transfers_base_erc20_rolling_hour') }}
)

SELECT
    b.blockchain,
    d.hour,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    b.symbol
FROM 
hourly_balances b
INNER JOIN 
hours d 
    ON b.hour <= d.hour 
    AND d.hour < b.next_hour
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND d.hour = p.minute
    AND p.blockchain = 'base'
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN 
{{ ref('balances_base_erc20_noncompliant') }} nc
    ON b.token_address = nc.token_address
WHERE nc.token_address IS NULL