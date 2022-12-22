{{ config(
        alias='erc721_day',
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
    tokenId,
    day,
    lead(day, 1, now()) OVER (PARTITION BY token_address, tokenId ORDER BY day) AS next_day
    FROM {{ ref('transfers_fantom_erc721_rolling_day') }})

SELECT distinct
    'fantom' as blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    nft_tokens.name as collection
FROM daily_balances b
INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
LEFT JOIN {{ ref('tokens_nft') }} nft_tokens ON nft_tokens.contract_address = b.token_address
AND nft_tokens.blockchain = 'fantom'
