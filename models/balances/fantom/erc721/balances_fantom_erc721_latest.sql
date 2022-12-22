{{ config(
        alias='erc721_latest',
        post_hook='{{ expose_spells_hide_trino(\'["fantom"]\',
                                            "sector",
                                            "balances",
                                            \'["Henrystats"]\') }}'
        )
}}
SELECT distinct
    wallet_address,
    token_address,
    tokenId,
    nft_tokens.name as collection,
    updated_at
FROM {{ ref('transfers_fantom_erc721_rolling_day') }}
LEFT JOIN {{ ref('tokens_nft') }} nft_tokens ON nft_tokens.contract_address = token_address
AND nft_tokens.blockchain = 'fantom'
WHERE recency_index = 1
