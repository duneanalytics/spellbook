{{ config(
        schema='prices_zksync',
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

    ('busd-binance-usd', 'zksync', 'BUSD', 0x2039bb4116b4efc145ec4f0e2ea75012d6c0f181, 18),
    ('eth-ethereum', 'zksync', 'ETH', 0x000000000000000000000000000000000000800a, 18),
    ('izi-izumi-finance', 'zksync', 'iZi', 0x16a9494e257703797d747540f01683952547ee5b, 18),
    ('mav-maverick-token', 'zksync', 'MAV', 0x787c09494ec8bcb24dcaf8659e7d5d69979ee508, 18),
    ('mute-mute', 'zksync', 'MUTE', 0x0e97c7a0f8b2c9885c8ac9fc6136e829cbc21d42, 18),
    ('lusd-liquity-usd', 'zksync', 'LUSD', 0x503234f203fc7eb888eec8513210612a43cf6115, 18),
    ('usdc-usd-coin', 'zksync', 'USDC.e', 0x3355df6d4c9c3035724fd0e3914de96a5a83aaf4, 6),
    ('usdt-tether', 'zksync', 'USDT.e', 0x493257fd37edb34451f62edf8d2a0c418852ba4c, 6),
    ('wbtc-wrapped-bitcoin', 'zksync', 'WBTC', 0xbbeb516fb02a01611cbbe0453fe3c580d7281011, 8),
    ('weth-weth', 'zksync', 'WETH', 0x5aea5775959fbc2557cc8789bc1bf90a239d9a91, 18),
    ('hold-holdstation', 'zksync', 'HOLD', 0xed4040fd47629e7c8fbb7da76bb50b3e7695f0f2, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
