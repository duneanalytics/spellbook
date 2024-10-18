{{
    config(
        schema = 'tokens_kaia'
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
    (0xcee8faf64bb97a73bb51e115aa89c17ffa8dd167, 'oUSDT', 6)
    , (0x34d21b1e550d73cee41151c77f3c73359527a396, 'oETH', 18)
    , (0x754288077d0ff82af7a5317c7cb8c444d421d103, 'oUSDC', 6)
) AS temp_table (contract_address, symbol, decimals)