{{ config(
        schema='prices_aptos',
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
    , contract_address
    , decimals
FROM
(
    VALUES
    ('thl-thala', 'aptos', 'THL', 0x7fd500c11216f0fe3095d0c4b8aa4d64a4e2e04f83758462f2b127255643615, 8),
 ) as temp (token_id, blockchain, symbol, contract_address, decimals)
