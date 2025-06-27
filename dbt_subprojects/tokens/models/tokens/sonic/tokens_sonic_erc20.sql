{{
    config(
        schema = 'tokens_sonic'
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
    (0x50c42dEAcD8Fc9773493ED674b675bE577f2634b, 'WETH', 18)
    , (0x773CDA0CADe2A3d86E6D4e30699d40bB95174ff2, 'waSonicSOLVBTC', 18)
) as temp (contract_address, symbol, decimals)