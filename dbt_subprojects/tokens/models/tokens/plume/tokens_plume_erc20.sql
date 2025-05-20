{{
    config(
        schema = 'tokens_plume'
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
    (0xEa237441c92CAe6FC17Caaf9a7acB3f953be4bd1, 'WPLUME', 18)
    , (0xda6087E69C51E7D31b6DBAD276a3c44703DFdCAd, 'USDT', 6)
    , (0x39d1F90eF89C52dDA276194E9a832b484ee45574, 'pETH', 18)
    , (0xca59cA09E5602fAe8B629DeE83FfA819741f14be, 'WETH', 18)
    , (0x78adD880A697070c1e765Ac44D65323a0DcCE913, 'USDC.e', 6)
) AS temp_table (contract_address, symbol, decimals)
