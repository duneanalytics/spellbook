{{ config(
        
        alias = 'erc20_day',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}
WITH 

time_seq AS (
    SELECT 
        sequence(
        CAST('2021-05-29' as timestamp),
        date_trunc('day', cast(now() as timestamp)),
        interval '1' day
        ) AS time 
),

days AS (
    SELECT 
        time.time AS block_day 
    FROM time_seq
    CROSS JOIN unnest(time) AS time(time)
),

daily_balances as (
    SELECT
        blockchain, 
        block_day, 
        wallet_address, 
        token_address, 
        amount_raw,
        amount,
        symbol,
        LEAD(block_day, 1, current_timestamp) OVER (PARTITION BY token_address, wallet_address ORDER BY block_day) AS next_day
    FROM 
    {{ ref('transfers_arbitrum_erc20_rolling_day') }}
)

SELECT
    b.blockchain,
    cast(date_trunc('month', d.block_day) as date) as block_month,
    d.block_day,
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
    ON b.block_day <= d.block_day 
    AND d.block_day < b.next_day
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND d.block_day = p.minute
    AND p.blockchain = 'arbitrum'
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN 
{{ ref('balances_arbitrum_erc20_noncompliant') }} nc
    ON b.token_address = nc.token_address
WHERE nc.token_address IS NULL
