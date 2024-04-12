{{ config(
        schema='prices_zora',
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
    ('weth-weth', 'zora', 'WETH', 0x4200000000000000000000000000000000000006, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
