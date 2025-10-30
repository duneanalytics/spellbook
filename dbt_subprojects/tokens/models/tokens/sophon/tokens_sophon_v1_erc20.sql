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
FROM 
(
    VALUES
    -- placeholder rows to give example of format, tokens missing in automated tokens.erc20
    (0x72af9F169B619D85A47Dfa8fefbCD39dE55c567D, 'ETH', 18)
    , (0x6386da73545ae4e2b2e0393688fa8b65bb9a7169, 'USDT', 6)
    , (0x9Aa0F72392B5784Ad86c6f3E899bCc053D00Db4F, 'USDC', 6)
) AS temp_table (contract_address, symbol, decimals)