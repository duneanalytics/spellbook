{{ config(
        alias = alias('erc721_day'),
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                        "sector",
                        "balances",
                        \'["hildobby","soispoke","dot2dotseurat"]\') }}',
                        tags=['dunesql']
        )
}}

WITH
    days AS (
        SELECT day
        FROM
            unnest(
                sequence(
                    date('2015-01-01'), date_trunc('day', cast(now() AS timestamp)), interval '1' day
                )
            ) AS t(day)
    )

, daily_balances AS
 (SELECT
    b.wallet_address,
    b.token_address,
    b.tokenId,
    b.day,
    b.amount,
    lead(b.day, 1, now()) OVER (PARTITION BY b.wallet_address, b.token_address, b.tokenId ORDER BY day) AS next_day
FROM {{ ref('transfers_ethereum_erc721_rolling_day') }} b
LEFT JOIN {{ ref('balances_ethereum_erc721_noncompliant') }} AS nc
    ON b.token_address = nc.token_address
WHERE nc.token_address IS NULL
)

SELECT
    'ethereum' AS blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    nft_tokens.name AS collection
FROM daily_balances b
INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
LEFT JOIN {{ ref('tokens_nft') }} nft_tokens ON nft_tokens.contract_address = b.token_address
AND nft_tokens.blockchain = 'ethereum'
WHERE b.amount = 1
;