{{ config(
        alias = 'erc1155_day',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke"]\') }}'
        )
}}

/*
    note: this spell has not been migrated to dunesql, therefore is only a view on spark
        please migrate to dunesql to ensure up-to-date logic & data
*/

with
    days as (
        select
            explode(
                sequence(
                    to_date('2015-01-01'), date_trunc('day', now()), interval 1 day
                )
            ) as day
    )

, daily_balances as
 (SELECT
    b.wallet_address,
    b.token_address,
    b.tokenId,
    b.day,
    b.amount, 
    lead(b.day, 1, now()) OVER (PARTITION BY b.wallet_address, b.token_address, b.tokenId ORDER BY day) AS next_day
FROM {{ ref('transfers_ethereum_erc1155_rolling_day') }} b
)

SELECT 
    'ethereum' as blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    SUM(b.amount) as amount, 
    nft_tokens.name as collection
FROM daily_balances b
INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
LEFT JOIN {{ ref('tokens_nft') }} nft_tokens ON nft_tokens.contract_address = b.token_address
AND nft_tokens.blockchain = 'ethereum'
GROUP BY 1, 2, 3, 4, 5, 7
HAVING SUM(b.amount) > 0