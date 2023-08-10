{{ config(
        schema='prices_optimism',
        alias = alias('tokens'),
        materialized='table',
        file_format = 'delta',
        tags = ['dunesql']
        )
}}
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
