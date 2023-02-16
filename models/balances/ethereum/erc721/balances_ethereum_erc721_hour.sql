{{ config(
        alias='erc721_hour',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby","soispoke","dot2dotseurat"]\') }}'
        )
}}

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
    tokenId,
    hour,
    lead(hour, 1, now()) OVER (PARTITION BY wallet_address, token_address, tokenId ORDER BY hour) AS next_hour
    FROM {{ ref('transfers_ethereum_erc721_rolling_hour') }}
    WHERE amount = 1)

SELECT 
    'ethereum' as blockchain,
    d.hour,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    nft_tokens.name as collection
FROM hourly_balances b
INNER JOIN hours d ON b.hour <= d.hour AND d.hour < b.next_hour
LEFT JOIN {{ ref('tokens_nft') }} nft_tokens ON nft_tokens.contract_address = b.token_address
AND nft_tokens.blockchain = 'ethereum'
LEFT JOIN {{ ref('balances_ethereum_erc721_noncompliant') }}  as nc
    ON b.token_address = nc.token_address
WHERE nc.token_address IS NULL 
