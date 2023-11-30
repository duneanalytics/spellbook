{{ config(
        schema = 'balances_base',
        alias = 'erc20_hour',
        post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "balances",
                                    \'["rantum"]\') }}'
        )
}}

WITH 

years as (
   SELECT date_add('year', CAST(n AS INTEGER), CAST(date '2023-06-15' AS TIMESTAMP)) AS year
    FROM UNNEST(sequence(0, date_diff('year', date '2023-06-15', current_date) )) AS t(n);
),

hours as (
    select date_add('hour', s.n, y.year) as block_hour
    from years y
      cross join unnest(sequence(1, 9000)) s(n)
    where s.n <= date_diff('hour', y.year, y.year + interval '1' year)
),

hourly_balances as (
    SELECT 
        blockchain, 
        block_hour, 
        wallet_address, 
        token_address, 
        amount_raw,
        amount,
        symbol,
        LEAD(block_hour, 1, current_timestamp) OVER (PARTITION BY token_address, wallet_address ORDER BY block_hour) AS next_hour
    FROM 
    {{ ref('transfers_base_erc20_rolling_hour') }}
)

SELECT
    b.blockchain,
    cast(date_trunc('month', d.block_hour) as date) as block_month,
    d.block_hour,
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
    ON b.block_hour <= d.block_hour 
    AND d.block_hour < b.next_hour
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND d.block_hour = p.minute
    AND p.blockchain = 'base'
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN 
{{ ref('balances_base_erc20_noncompliant') }} nc
    ON b.token_address = nc.token_address
WHERE nc.token_address IS NULL