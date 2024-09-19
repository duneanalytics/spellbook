{{ config(
        schema='prices_optimism',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        
        )
}}

WITH op AS (
    SELECT
        token_id
        , blockchain
        , symbol
        , contract_address
        , decimals
    FROM {{ ref('prices_optimism_tokens_curated') }}
    UNION ALL
    SELECT
        token_id
        , blockchain
        , symbol
        , contract_address
        , decimals
    FROM {{ ref('prices_optimism_tokens_bridged') }}
    WHERE contract_address not in (select contract_address from {{ ref('prices_optimism_tokens_curated') }})
)
SELECT
    *
FROM
    op
WHERE
    -- safeguard as native tokens currently have null address
    -- prices_optimism_tokens_bridged has tx's which incorrectly output 0x0 address
    contract_address IS DISTINCT FROM 0x0000000000000000000000000000000000000000