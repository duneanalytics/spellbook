{{ config(
        schema='prices_cardano',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}
SELECT 
    token_id
    , blockchain
    , symbol
    , CAST(null as VARBINARY) as contract_address
    , CAST(null as int) as decimals
FROM
(
    VALUES
    ('snek-snek-crd', 'cardano', 'SNEK')
) as temp (token_id, blockchain, symbol)
