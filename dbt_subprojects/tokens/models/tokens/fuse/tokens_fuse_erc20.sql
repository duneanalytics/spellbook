{{
    config(
        schema = 'tokens_fuse'
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
    (0x34ef2cc892a88415e9f02b91bfa9c91fc0be6bd4, 'VOLT', 18),
    (0x0be9e53fd7edac9f859882afdda116645287c629, 'WFUSE', 18),
    (0xb1dd0b683d9a56525cc096fbf5eec6e60fe79871, 'sFUSE', 18)    
) AS temp_table (contract_address, symbol, decimals)