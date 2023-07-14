{{ config(
        schema='prices_optimism',
        alias = alias('tokens', legacy_model=True),
        materialized='table',
        file_format = 'delta',
        tags=['legacy', 'static']
        )
}}
SELECT
    token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM {{ ref('prices_optimism_tokens_curated_legacy') }}
UNION ALL
SELECT
     token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM {{ ref('prices_optimism_tokens_bridged_legacy') }}
WHERE contract_address not in (select contract_address from {{ ref('prices_optimism_tokens_curated_legacy') }})
