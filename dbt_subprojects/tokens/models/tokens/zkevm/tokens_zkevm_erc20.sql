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
) AS temp_table (contract_address, symbol, decimals)