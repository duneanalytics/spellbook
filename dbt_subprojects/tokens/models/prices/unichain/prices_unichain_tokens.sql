{% set blockchain = 'unichain' %}

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
    ('weth-weth', 'WETH', 0x4200000000000000000000000000000000000006, 18)
    , ('usdc-usd-coin', 'USDC', 0x078d782b760474a361dda0af3839290b0ef57ad6, 6)
    , ('uni-uniswap', 'UNI', 0x8f187aa05619a017077f5308904739877ce9ea21, 18)
    , ('dai-dai', 'DAI', 0x20cab320a855b39f724131c69424240519573f81, 18)
    , ('wsteth-wrapped-liquid-staked-ether-20', 'wstETH', 0xc02fe7317d4eb8753a02c35fe019786854a92001, 18)
    , ('usdt-tether', 'USDT', 0x588ce4f028d8e7b53b687865d6a67b3a54c75518, 6)
    , ('usdt-tether', 'USDT0', 0x9151434b16b9763660705744891fA906F660EcC5, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x927B51f251480a681271180DA4de28D44EC4AfB8, 8)
    , ('weeth-wrapped-eeth', 'weETH', 0x7DCC39B4d1C53CB31e1aBc0e358b43987FEF80f7, 18)
    , ('ezeth-renzo-restaked-eth', 'ezETH', 0x2416092f143378750bb29b79eD961ab195CcEea5, 18)
    , ('rseth-rseth', 'rsETH', 0xc3eACf0612346366Db554C991D7858716db09f58, 18)
    , ('comp-compoundd', 'COMP', 0xdf78e4F0A8279942ca68046476919A90f2288656, 18)
    , ('wbtc-wrapped-bitcoin', 'kBTC', 0x73e0c0d45e048d25fc26fa3159b0aa04bfa4db98, 8)
) as temp (token_id, symbol, contract_address, decimals) 
