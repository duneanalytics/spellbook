{{ config(
        alias = 'erc721_noncompliant'
) 
}}

WITH

    erc721_transfers AS (
        SELECT
            tr.blockchain,
            tr.to AS wallet_address,
            tr.contract_address AS token_address,
            tr.token_id AS tokenId,
            row_number() over (partition by tr.contract_address, tr.token_id order by tr.block_time desc, tr.evt_index desc) as recency_index
        FROM {{ ref('nft_transfers') }} tr
        WHERE TRUE
            AND tr.blockchain = 'ethereum'
            AND tr.token_standard = 'erc721'
    )

    , multiple_owners AS (
        SELECT DISTINCT
            token_address,
            tokenId
        FROM erc721_transfers
        WHERE recency_index = 1
        GROUP BY blockchain, token_address, tokenId
        HAVING COUNT(wallet_address) > 1
    )

    SELECT * FROM multiple_owners