{{ config(
        schema='prices_base',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags=['static']
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
    ('usdb-usdb', 'blast', 'USDB', '0x4300000000000000000000000000000000000003', 18),
    ('weth-weth', 'blast', 'WETH', '0x4300000000000000000000000000000000000004', 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)