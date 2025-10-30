{{
    config(
        schema = 'tokens_flare'
        , alias = 'erc20'
        , tags = ['static']
        , materialized = 'table'
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
    (0x1D80c49BbBCd1C0911346656B529DF9E5c2F783d, 'WFLR', 18)
) as temp (contract_address, symbol, decimals)