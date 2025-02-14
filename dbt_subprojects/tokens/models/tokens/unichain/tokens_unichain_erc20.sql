{{
    config(
        schema = 'tokens_unichain'
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
    , (0x078d782b760474a361dda0af3839290b0ef57ad6, 'USDC', 6)
    , (0x8f187aa05619a017077f5308904739877ce9ea21, 'UNI', 18)
    , (0x20cab320a855b39f724131c69424240519573f81, 'DAI', 18)
) AS temp_table (contract_address, symbol, decimals) 