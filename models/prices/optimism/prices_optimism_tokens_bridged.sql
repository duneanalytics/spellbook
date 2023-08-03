{{ config(
        schema='prices_optimism',
        alias = alias('tokens_bridged'),
        materialized='table',
        file_format = 'delta',
        tags=['static']
        )
}}
SELECT
    e.token_id
    , 'optimism' as blockchain
    , e.symbol as symbol
    , o.l2_token as contract_address
    , e.decimals
FROM {{ ref('tokens_optimism_erc20_bridged_mapping') }} o
INNER JOIN {{ ref('prices_ethereum_tokens') }} e
ON e.contract_address = o.l1_token
