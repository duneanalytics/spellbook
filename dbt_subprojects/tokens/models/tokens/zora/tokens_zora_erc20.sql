{{
    config(
        schema = 'tokens_zora'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM (VALUES
        (0x4200000000000000000000000000000000000006, 'WETH', 18)
        , (0xCccCCccc7021b32EBb4e8C08314bD62F7c653EC4, 'USDzC', 6)
        , (0xa6B280B42CB0b7c4a4F789eC6cCC3a7609A1Bc39, 'ENJOY', 18)
        , (0x078540eECC8b6d89949c9C7d5e8E91eAb64f6696, 'Imagine', 18)
) AS temp_table (contract_address, symbol, decimals)