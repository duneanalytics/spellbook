{{ config(
        alias='erc721_latest'
        )
}}
SELECT
    wallet_address,
    token_address,
    amount,
    tokenId,
    last_updated
FROM {{ ref('transfers_ethereum_erc721_rolling_hour') }}