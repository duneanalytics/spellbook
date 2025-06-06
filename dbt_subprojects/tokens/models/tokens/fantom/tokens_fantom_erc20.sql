{{
    config(
        schema = 'tokens_fantom'
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
    -- placeholder rows to give example of format, tokens already exist in tokens.erc20
    (0x91b39d5584e2a7dc829f696235742cc293f2e8cf, 'SPOOKY-LP(BAND-WFTM)', 18)
    , (0x4e415957aa4fd703ad701e43ee5335d1d7891d83, 'BPT-DEIUSDC', 18)
)
AS temp_table (contract_address, symbol, decimals)