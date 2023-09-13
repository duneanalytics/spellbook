{{ config(
        tags = ['dunesql'],
        alias = alias('bep20_hour'),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

WITH 

-- credits @tomfutago - https://dune.com/queries/2988360

years as (
    select year
    from (values (sequence(timestamp '2020-08-29', cast(date_trunc('year', now()) as timestamp), interval '1' year))) s(year_array)
      cross join unnest(year_array) as d(year)
),

hours as (
    select date_add('hour', s.n, y.year) as block_hour
    from years y
      cross join unnest(sequence(0, 9000)) s(n)
    where s.n <= date_diff('hour', y.year, y.year + interval '1' year)
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
    {{ ref('transfers_bnb_bep20_rolling_hour') }}
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
    ON b.hour <= d.block_hour 
    AND d.block_hour < b.next_hour
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND d.block_hour = p.minute
    AND p.blockchain = 'bnb'
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN 
{{ ref('balances_bnb_bep20_noncompliant') }} nc
    ON b.token_address = nc.token_address
WHERE nc.token_address IS NULL