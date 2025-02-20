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
) AS temp_table (contract_address, symbol, decimals) 