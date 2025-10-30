{{
    config(
        schema = 'tokens_celo'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address as contract_address
    , trim(symbol) as symbol
    , decimals
FROM
(
    VALUES
    -- placeholder rows to give example of format, tokens already exist in tokens.erc20
    (0x696aeeaa0d7c039d1c2fd410bceae391c739186c, 'SYRUP', 18)
    , (0xad59c75eb9568e699dfc71d530e27f8d12e135ca, 'cXOF', 8)
)
AS temp_table (contract_address, symbol, decimals)