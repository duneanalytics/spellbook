{{
    config(
        schema = 'tokens_corn'
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
    (0xda5ddd7270381a7c2717ad10d1c0ecb19e3cdfb2, 'wBTCN', 18)
    , (0xecAc9C5F704e954931349Da37F60E39f515c11c1, 'LBTC', 8)
    , (0xDF0B24095e15044538866576754F3C964e902Ee6, 'USDC.e', 6)
    , (0xF469fBD2abcd6B9de8E169d128226C0Fc90a012e, 'pumpBTC', 8)
) as temp (contract_address, symbol, decimals)
