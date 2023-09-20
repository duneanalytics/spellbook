{{ config(
    alias = alias('erc721_noncompliant'),
    tags=['dunesql'])
}}

WITH
multiple_owners AS (
    SELECT
        blockchain,
        token_address,
        tokenId,
        count(wallet_address) AS holder_count --should always be 1
    FROM {{ ref('transfers_ethereum_erc721_rolling_day') }}
    WHERE recency_index = 1
    AND amount = 1
    GROUP BY blockchain, token_address, tokenId
    HAVING count(wallet_address) > 1
)

SELECT DISTINCT token_address AS token_address FROM multiple_owners