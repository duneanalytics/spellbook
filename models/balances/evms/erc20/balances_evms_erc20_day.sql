{{ config(
        schema = 'balances_evms',
        alias = alias('erc20_day'),
        tags = ['dunesql'],
        partition_by = ['blockchain', 'day'],
        post_hook='{{ expose_spells(\'["evms"]\',
                                        "sector",
                                        "balances",
                                        \'["longnhbkhn"]\') }}'
        )
}}


with 
    days as (
        with list_day as (select sequence(date('2015-01-01'),  date(now()), interval '1' day) as day)

        select u.day from list_day cross join unnest(day) as u(day)
    )
  , daily_balances as
 (SELECT
    blockchain,
    token_address,
    symbol,
    wallet_address,
    amount_raw,
    amount,
    amount_transfer_usd,
    day,
    lead(day, 1, date(now())) OVER (PARTITION BY blockchain, token_address, wallet_address ORDER BY day) AS next_day
    FROM {{ ref('transfers_evms_erc20_rolling_day') }})

SELECT
    b.blockchain,
    d.day,
    b.token_address,
    b.symbol,
    b.wallet_address,
    b.amount_raw,
    b.amount,
    p.price as price_btc,
    b.amount_transfer_usd as profit,
    b.amount * p.price as amount_usd,
    b.amount * p.price + b.amount_transfer_usd as total_asset
FROM daily_balances b
INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
LEFT JOIN {{ source('prices', 'usd') }} p
    ON d.day = p.minute
    and b.blockchain = p.blockchain
    and b.token_address = p.contract_address
