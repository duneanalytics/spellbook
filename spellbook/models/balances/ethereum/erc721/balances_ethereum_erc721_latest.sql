{{ config(
        alias='erc721_latest'
        )
}}
SELECT distinct
    wallet_address,
    token_address,
    tokenId,
    nft_tokens.name as collection,
    current_time() as updated_at
FROM {{ ref('transfers_ethereum_erc721') }}
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON nft_tokens.contract_address = token_address
group bu 1,2,3,4
having sum(amount) = 1