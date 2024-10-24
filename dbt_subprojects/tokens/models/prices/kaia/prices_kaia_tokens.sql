{% set blockchain = 'kaia' %}

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
    --below 10 from: https://kaiascope.com/tokens?sort=descVolume
    ('usdt-tether', 'oUSDT', 0xcee8faf64bb97a73bb51e115aa89c17ffa8dd167, 6)
    , ('eth-ethereum', 'oETH', 0x34d21b1e550d73cee41151c77f3c73359527a396, 18)
    , ('usdc-usd-coin', 'oUSDC', 0x754288077d0ff82af7a5317c7cb8c444d421d103, 6)
    , ('bnb-binance-coin', 'oBNB', 0x574e9c26bda8b95d7329505b4657103710eb32ea, 18)
    , ('xrp-xrp', 'oXRP', 0x9eaefb09fe4aabfbe6b1ca316a3c36afc83a393f, 6)
    , ('busd-binance-usd', 'oBUSD', 0x210bc03f49052169d5588a52c317f71cf2078b85, 18)
    , ('bora-bora', 'BORA', 0x02cbe46fb8a1f579254a9b485788f2d86cad51aa, 18)
    --below 14 from: https://coinpaprika.com/tag/klaytn-token/home-overview/?all=true
    , ('mbx-marblex', 'MBX', 0xd068c52d81f4409b9502da926ace3301cc41f623, 18)
    , ('isk-iskra-token', 'ISK', 0x17d2628d30f8e9e966c9ba831c9b9b01ea8ea75c, 18)
    , ('ksp-klayswap-protocol', 'KSP', 0xc6a2ad8cc6e4a7e08fc37cc5954be07d499e7654, 18)
    , ('wiken-project-with', 'WIKEN', 0x275f942985503d8ce9558f8377cc526a3aba3566, 18)
    , ('dice-klaydice', 'DICE', 0x089ebd525949ee505a48eb14eecba653bc8d1b97, 18)
    , ('hibs-hiblocks', 'HIBS', 0xe06b40df899b9717b4e6b50711e1dc72d08184cf, 18)
    , ('selo-selo', 'SELO', 0x342633c4a7f91094096e15b513f039af52e960d9, 8)
    , ('clb-cloudbric', 'CLBK', 0xc4407f7dc4b37275c9ce0f839652b393e13ff3d1, 18)
    , ('jum-jumoney', 'JUM', 0x3eef2b8d8050197e409b69f09462f49eab562979, 18)
    , ('mooi-mooi-network', 'MOOI', 0x4b734a4d5bf19d89456ab975dfb75f02762dda1d, 18)
    , ('kai-kai-protocol', 'KAI', 0xe950bdcfa4d1e45472e76cf967db93dbfc51ba3e, 18)
    , ('gram-norma-in-metaland', 'GRAM', 0x02518a2a6af8133b59f69a8c162f112f76bb0d96, 18)
    , ('box-box', 'BOX', 0x656f86dd0f3bc25af2d15855f2a2f142f9eaed87, 18)
    , ('mdus-medieus', 'MDUS', 0xab9cb20a28f97e189ca0b666b8087803ad636b3c, 18)
) as temp (token_id, symbol, contract_address, decimals)