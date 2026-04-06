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
    ('hbar-hedera-hashgraph', 'WHBAR', 0x0000000000000000000000000000000000163b5a, 8)
    , ('usdc-usd-coin', 'USDC', 0x000000000000000000000000000000000006f89a, 6)
    , ('usdt-tether', 'USDT', 0x00000000000000000000000000000000000ec585, 6)
    , ('sauce-saucerswap', 'SAUCE', 0x00000000000000000000000000000000000b2ad5, 6)
    , ('hbarx-hbarx', 'HBARX', 0x00000000000000000000000000000000000cba44, 8)
    , ('karate-karate-combat', 'KARATE', 0x00000000000000000000000000000000000e6a7c, 8)
    , ('dovu-dovu', 'DOVU', 0x000000000000000000000000000000000038b3db, 8)
    , ('pack-hashpack', 'PACK', 0x00000000000000000000000000000000002dede3, 8)
    , ('hsuite-hsuite', 'HSUITE', 0x00000000000000000000000000000000001647e8, 8)
) as temp (token_id, symbol, contract_address, decimals)
