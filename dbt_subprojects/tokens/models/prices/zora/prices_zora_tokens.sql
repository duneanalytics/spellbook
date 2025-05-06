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
    , ('usdc-usd-coin', 'zora', 'USDzC', 0xCccCCccc7021b32EBb4e8C08314bD62F7c653EC4, 6)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
