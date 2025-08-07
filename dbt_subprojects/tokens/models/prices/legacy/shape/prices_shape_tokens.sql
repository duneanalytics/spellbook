{{ config(
        schema='prices_shape',
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
    ('weth-weth','shape','WETH',0x4200000000000000000000000000000000000006,18),
    ('usdc-usd-coin','shape','USDC.e',0xdb7DD8B00EdC5778Fe00B2408bf35C7c054f8BBe,6)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
