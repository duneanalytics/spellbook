{% set blockchain = 'hedera' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

SELECT
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    ('whbar-wrapped-hbar', 'WHBAR', 0x0000000000000000000000000000000000163b5a, 8)
    , ('usdc-usd-coin', 'USDC', 0x000000000000000000000000000000000006f89a, 6)
    , ('usdt0-usdt0', 'USDT0', 0x00000000000000000000000000000000009ce723, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x000000000000000000000000000000000099d925, 8)
    , ('sauce-saucerswap', 'SAUCE', 0x00000000000000000000000000000000000b2ad5, 6)
    , ('pack-hashpack', 'PACK', 0x0000000000000000000000000000000000492a28, 6)
    , ('dovu-dovu', 'DOVU', 0x000000000000000000000000000000000038b3db, 8)
) as temp (token_id, symbol, contract_address, decimals)
