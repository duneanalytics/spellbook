{{ config(
        alias='erc721_latest',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby","soispoke","dot2dotseurat"]\') }}'
        )
}}
SELECT distinct
    wallet_address,
    token_address,
    tokenId,
    nft_tokens.name as collection,
    updated_at
FROM {{ ref('transfers_ethereum_erc721_rolling_day') }}
LEFT JOIN {{ ref('tokens_nft') }} nft_tokens ON nft_tokens.contract_address = token_address
AND nft_tokens.blockchain = 'ethereum'
WHERE recency_index = 1
