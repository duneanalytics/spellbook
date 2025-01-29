{{
    config(
        schema = 'tokens_boba'
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
    (0xa18bf3994c0cc6e3b63ac420308e5383f53120d7, 'BOBA', 18)
    , (0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000, 'ETH', 18)
    , (0x66a2a913e447d6b4bf33efbec43aaef87890fbbc, 'USDC', 6)
    , (0x5de1677344d3cb0d7d465c10b72a8f60699c062d, 'USDT', 6)
    , (0xf74195bb8a5cf652411867c5c2c5b8c2a402be35, 'DAI', 18)
    , (0xd203de32170130082896b4111edf825a4774c18e, 'WETH', 18)
) as temp (contract_address, symbol, decimals)
