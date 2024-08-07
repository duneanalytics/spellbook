{{
    config(
        schema = 'tokens_nova'
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
    (0xf823C3cD3CeBE0a1fA952ba88Dc9EEf8e0Bf46AD, 'ARB', 18)
    , (0x750ba8b76187092B0D1E87E28daaf484d1b5273b, 'USDC', 6)
    , (0x1d05e4e72cD994cdF976181CfB0707345763564d, 'WBTC', 8)
    , (0x722E8BdD2ce80A4422E880164f2079488e115365, 'WETH', 18)
    , (0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1, 'DAI', 18)
) AS temp_table (contract_address, symbol, decimals)