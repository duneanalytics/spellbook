{{
    config(
        schema = 'tokens_berachain'
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
    (0x6969696969696969696969696969696969696969, 'WBERA', 18)
    , (0x549943e04f40284185054145c6E4e9568C1D3241, 'USDC.e', 6)
    , (0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590, 'WETH', 18)
    , (0x657e8C867D8B37dCC18fA4Caead9C45EB088C642, 'eBTC', 8)
    , (0x0555E30da8f98308EdB960aa94C0Db47230d2B9c, 'WBTC', 8)
    , (0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34, 'USDe', 18)
    , (0xecAc9C5F704e954931349Da37F60E39f515c11c1, 'LBTC', 8)
    , (0x4186BFC76E2E237523CBC30FD220FE055156b41F, 'rsETH', 18)
    , (0xfcbd14dc51f0a4d49d5e53c2e0950e0bc26d0dce, 'HONEY', 18)
    , (0x656b95E550C07a9ffe548bd4085c72418Ceb1dba, 'BGT', 18)
) AS temp_table (contract_address, symbol, decimals) 
