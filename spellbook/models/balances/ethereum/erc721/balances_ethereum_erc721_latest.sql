{{ config(
        alias='erc721_latest'
        )
}}
SELECT distinct
    wallet_address,
    token_address,
    tokenId,
    nft_tokens.name as collection,
    updated_at
FROM {{ ref('transfers_ethereum_erc721_rolling_hour') }}
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON nft_tokens.contract_address = token_address
