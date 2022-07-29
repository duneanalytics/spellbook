{{ config(
        alias='erc721_latest',
        )
}}

SELECT
    t.wallet_address,
    t.token_address,
    t.tokenId,
    nft_tokens.name as collection,
    now() as updated_at
FROM  {{ref('transfers_ethereum_erc721')}} as  t
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft_tokens ON nft_tokens.contract_address = t.token_address
group by 1,2,3,4,5
having sum(amount) = 1