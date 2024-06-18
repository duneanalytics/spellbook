{{ config(
        schema='prices_blast',
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
    ('weth-weth','blast','WETH',0x4300000000000000000000000000000000000004,18),
    ('usdb-usdb','blast','USDB',0x4300000000000000000000000000000000000003,18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
