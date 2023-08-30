{{ config(
        alias = alias('satoshi_day', legacy_model=True),
        tags = ['legacy'],
        post_hook='{{ expose_spells_hide_trino(\'["bitcoin"]\',
                                            "sector",
                                            "balances",
                                            \'["longnhbkhn"]\') }}'
        )
}}


with
    days as ( 
        select
            explode(
                sequence(
                    to_date('2009-01-03'), date_trunc('day', now()), interval 1 day
                )
            ) as day
    )

, daily_balances as
 (SELECT
    wallet_address,
    amount_raw,
    amount_raw * power(10, -8) as amount,
    day,
    day + interval '1 day' AS next_day
    FROM {{ ref('transfers_bitcoin_satoshi_rolling_day_legacy') }})

SELECT
    'bitcoin' as blockchain,
    d.day,
    b.wallet_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
FROM daily_balances b
INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
LEFT JOIN {{ source('prices', 'usd') }} p
    ON d.day = p.minute
    AND p.symbol='BTC'
