{{
    config(
        schema = 'tokens_hedera_v1'
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
    (0x0000000000000000000000000000000000163b5a, 'WHBAR', 8)
    , (0x000000000000000000000000000000000006f89a, 'USDC', 6)
    , (0x00000000000000000000000000000000009ce723, 'USDT0', 6)
    , (0x000000000000000000000000000000000099d925, 'WBTC', 8)
    , (0x00000000000000000000000000000000000b2ad5, 'SAUCE', 6)
    , (0x0000000000000000000000000000000000492a28, 'PACK', 6)
    , (0x000000000000000000000000000000000038b3db, 'DOVU', 8)
) as temp (contract_address, symbol, decimals)
