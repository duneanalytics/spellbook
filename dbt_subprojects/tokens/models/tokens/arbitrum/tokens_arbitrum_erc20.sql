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
    -- placeholder rows to give example of format, tokens already exist in tokens.erc20
    (0xa98c94d67d9df259bee2e7b519df75ab00e3e2a8, 'bwAJNA', 18)
    , (0xda492c29d88ffe9b7cbfa6dc068c2f9befae851b, 'CUSDCLP', 18)
)
AS temp_table (contract_address, symbol, decimals)