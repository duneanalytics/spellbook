{{
    config(
        schema = 'tokens_lens'
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
    (0x6bDc36E20D267Ff0dd6097799f82e78907105e2F, 'WGHO', 18)
) AS temp_table (contract_address, symbol, decimals)