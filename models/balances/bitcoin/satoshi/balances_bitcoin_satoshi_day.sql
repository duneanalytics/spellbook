{{ config(
        alias = alias('satoshi_day'),
        tags = ['dunesql'],
        post_hook='{{ expose_spells_hide_trino(\'["bitcoin"]\',
                                            "sector",
                                            "balances",
                                            \'["longnhbkhn"]\') }}'
        )
}}


with daily_balances as
 (SELECT
    wallet_address,
    amount_raw,
    amount_raw as amount,
    day,
    day + interval '1' day AS next_day
    FROM {{ ref('transfers_bitcoin_satoshi_rolling_day') }})

SELECT
    'bitcoin' as blockchain,
    b.day,
    b.wallet_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd
FROM daily_balances b
LEFT JOIN {{ source('prices', 'usd') }} p
    ON b.next_day = p.minute
    AND p.symbol='BTC'
    AND p.blockchain is null
