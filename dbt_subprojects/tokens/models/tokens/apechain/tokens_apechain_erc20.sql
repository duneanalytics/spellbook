{{
    config(
        schema = 'tokens_apechain'
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
    (0x48b62137edfa95a428d35c09e44256a739f6b557, 'WAPE', 18)
    , (0xe31c676d8235437597581b44c1c4f8a30e90b38a, 'GNS', 18)
    , (0xfc7b0badb1404412a747bc9bb6232e25098be303, 'APE', 18)
    , (0xcf800f4948d16f23333508191b1b1591daf70438, 'ApeETH', 18)
    , (0xa2235d059f80e176d931ef76b6c51953eb3fbef4, 'ApeUSD', 18)
) AS temp_table (contract_address, symbol, decimals)
