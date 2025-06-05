{{
    config(
        schema = 'tokens_arbitrum'
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
    -- tokens which don't appear in automated source, edge cases only
    (0xc3abc47863524ced8daf3ef98d74dd881e131c38, 'LUA', 18)
    , (0x5402b5f40310bded796c7d0f3ff6683f5c0cffdf, 'sGLP', 18)
    , (0xd07d35368e04a839dee335e213302b21ef14bb4a, 'CRYSTAL', 18)
)
AS temp_table (contract_address, symbol, decimals)
