{{
    config(
        schema = 'tokens_zkevm'
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
    (0xca5d8f8a8d49439357d3cf46ca2e720702f132b8, 'GYD', 18)
    , (0x1509706a6c66ca549ff0cb464de88231ddbe213b, 'AURA', 18)
    , (0x744c5860ba161b5316f7e80d9ec415e2727e5bd5, 'DAI', 18)
    , (0xb23c20efce6e24acca0cef9b7b7aa196b84ec942, 'rETH', 18)
    , (0x550d3bb1f77f97e4debb45d4f817d7b9f9a1affb, 'woUSDT', 6)
    , (0x120ef59b80774f02211563834d8e3b72cb1649d6, 'BAL', 18)
    , (0x12d8ce035c5de3ce39b1fdd4c1d5a745eaba3b8c, 'ankrETH', 18)
    , (0xa2036f0538221a77a3937f1379699f44945018d0, 'MATIC', 18)
    , (0x4f9a0e7fd2bf6067db6994cf12e4495df938e6e9, 'WETH', 18)
    , (0x22b21beddef74fe62f031d2c5c8f7a9f8a4b304d, 'POL', 18)
    , (0x68791cfe079814c46e0e25c19bcc5bfc71a744f7, 'AAVE', 18)
    , (0x4b16e4752711a7abec32799c976f3cefc0111f2b, 'LINK', 18)
    , (0xea034fb02eb1808c2cc3adbc15f447b93cbe08e1, 'WBTC', 8)
) AS temp_table (contract_address, symbol, decimals)
