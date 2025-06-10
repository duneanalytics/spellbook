{{
    config(
        schema = 'tokens_scroll'
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
    (0x60d01ec2d5e98ac51c8b4cf84dfcce98d527c747, 'iZi', 18)
    , (0xca77eb3fefe3725dc33bccb54edefc3d9f764f97, 'DAI', 18)
)
AS temp_table (contract_address, symbol, decimals)