{{
    config(
        schema = 'tokens_sophon'
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
    (0x000000000000000000000000000000000000800A, 'SOPH', 18)
    , (0x72af9F169B619D85A47Dfa8fefbCD39dE55c567D, 'ETH', 18)
) AS temp_table (contract_address, symbol, decimals) 