{{ config(
        schema='prices_optimism',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        
        )
}}
-- stamp 1
SELECT
    token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM {{ ref('prices_optimism_tokens_curated') }}
