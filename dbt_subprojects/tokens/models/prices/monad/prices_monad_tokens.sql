{% set blockchain = 'monad' %}

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
      ('usdc-usd-coin', 'USDC', 0x754704Bc059F8C67012fEd69BC8A327a5aafb603, 6)
    , ('ausd-agora-dollar', 'AUSD', 0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a, 6)
    , ('weth-weth', 'WETH', 0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242, 18)
    , ('sol-solana', 'SOL', 0xea17E5a9efEBf1477dB45082d67010E2245217f1, 9)
    , ('wmon-wrapped-monad', 'WMON', 0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A, 18)
    , ('usdt-tether', 'USDT0', 0xe7cd86e13ac4309349f30b3435a9d337750fc82d, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0x0555E30da8f98308EdB960aa94C0Db47230d2B9c, 8)
    , ('wsteth-wrapped-liquid-staked-ether-20', 'wstETH', 0x10Aeaf63194db8d453d4D85a06E5eFE1dd0b5417, 18)
    , ('xaut-tether-gold', 'XAUt0', 0x01bFF41798a0BcF287b996046Ca68b395DbC1071, 6)
) as temp (token_id, symbol, contract_address, decimals)
