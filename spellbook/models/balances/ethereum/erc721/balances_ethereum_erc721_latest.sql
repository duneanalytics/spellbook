{{ config(
        alias='erc721_latest'
        )
}}
SELECT
    wallet_address,
    token_address,
    tokenId,
    updated_at
FROM {{ ref('transfers_ethereum_erc721_rolling_hour') }}