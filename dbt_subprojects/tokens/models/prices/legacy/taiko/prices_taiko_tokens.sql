{{ config(
        schema='prices_taiko',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}
SELECT
    token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    ('usdc-usd-coin', 'taiko', 'USDC', 0x07d83526730c7438048d55a4fc0b850e2aab6f0b, 6),
    ('weth-weth', 'taiko', 'WETH', 0xa51894664a773981c6c112c43ce576f315d5b1b6, 18),
    ('taiko-taiko', 'taiko', 'TAIKO', 0xa9d23408b9ba935c230493c40c73824df71a0975, 18),
    ('usdce-usd-coine', 'taiko', 'USDC.e', 0x19e26b0638bf63aa9fa4d14c6baf8d52ebe86c5c, 6),
    ('usdt-tether', 'taiko', 'USDT', 0x2def195713cf4a606b49d07e520e22c17899a736, 6)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
