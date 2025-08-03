{{
    config(
        schema = 'tokens_goerli'
        , alias = 'erc20'
        , tags = ['static']
        , materialized = 'table'
    )
}}

SELECT contract_address
    , symbol
    , decimals
FROM 
(
    VALUES
    -- placeholder rows to give example of format, goerli not included in tokens.erc20
    (0xa3e0Dfbf8DbD86e039f7CDB37682A776D66dae4b, 'USDC', 6)
)
AS temp_table (contract_address, symbol, decimals)