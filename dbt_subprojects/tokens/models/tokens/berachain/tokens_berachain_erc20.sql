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
    (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'USDe', 18)
    , (0x7dcc39b4d1c53cb31e1abc0e358b43987fef80f7, 'weETH', 18)
    , (0xecac9c5f704e954931349da37f60e39f515c11c1, 'LBTC', 8)
) AS temp_table (contract_address, symbol, decimals) 