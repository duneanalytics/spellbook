{{ config(
        alias='ftm_hour',
        post_hook='{{ expose_spells_hide_trino(\'["fantom"]\',
                                            "sector",
                                            "balances",
                                            \'["Henrystats"]\') }}'
        )
}}

with
    hours as (
        select
            explode(
                sequence(
                    to_date('2019-12-27'), date_trunc('hour', now()), interval 1 hour
                )
            ) as hour
    )

, hourly_balances as
 (SELECT
    wallet_address,
    token_address,
    amount_raw,
    amount,
    hour,
    symbol,
    lead(hour, 1, now()) OVER (PARTITION BY token_address, wallet_address ORDER BY hour) AS next_hour
    FROM {{ ref('transfers_fantom_ftm_rolling_hour') }})

SELECT
    'fantom' as blockchain,
    h.hour,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    'FTM' as symbol -- change symbol to ftn 
FROM hourly_balances b
INNER JOIN hours h ON b.hour <= h.hour AND h.hour < b.next_hour
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND h.hour = p.minute
    AND p.blockchain = 'fantom'
