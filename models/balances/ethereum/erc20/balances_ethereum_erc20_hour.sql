{{ config(
        alias = alias('erc20_hour'),
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke","dot2dotseurat"]\') }}'
        )
}}

/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/

with
    hours as (
        select
            explode(
                sequence(
                    to_date('2015-01-01'), date_trunc('hour', now()), interval 1 hour
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
    FROM {{ ref('transfers_ethereum_erc20_rolling_hour') }})

SELECT
    'ethereum' as blockchain,
    h.hour,
    b.wallet_address,
    b.token_address,
    b.amount_raw,
    b.amount,
    b.amount * p.price as amount_usd,
    b.symbol
FROM hourly_balances b
INNER JOIN hours h ON b.hour <= h.hour AND h.hour < b.next_hour
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.contract_address = b.token_address
    AND h.hour = p.minute
    AND p.blockchain = 'ethereum'
-- Removes rebase tokens from balances
LEFT JOIN {{ ref('tokens_ethereum_rebase') }}  as r
    ON b.token_address = r.contract_address
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN {{ ref('balances_ethereum_erc20_noncompliant') }}  as nc
    ON b.token_address = nc.token_address
WHERE r.contract_address is null
and nc.token_address is null
