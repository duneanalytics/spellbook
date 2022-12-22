{{ config(
        alias='ftm_day',
        post_hook='{{ expose_spells_hide_trino(\'["fantom"]\',
                                            "sector",
                                            "balances",
                                            \'["Henrystats"]\') }}'
        )
}}

with
    days as (
        select
            explode(
                sequence(
                    to_date('2019-12-27'), date_trunc('day', now()), interval 1 day
                )
            ) as day
    )

, daily_balances as
 (SELECT
    wallet_address,
    token_address,
    amount_raw,
    amount,
    day,
    symbol,
    lead(day, 1, now()) OVER (PARTITION BY token_address, wallet_address ORDER BY day) AS next_day
    FROM {{ ref('transfers_fantom_ftm_rolling_day') }})

SELECT
    'fantom' as blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    'FTM' as symbol -- change symbol to ftm 
FROM daily_balances b
INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND d.day = p.minute
    AND p.blockchain = 'fantom'
