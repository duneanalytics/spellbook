{{
    config(
        schema = 'tokens_kaia'
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
    (0x5c13e303a62fc5dedf5b52d66873f2e59fedadc2, 'USDT', 6)
    , (0xcee8faf64bb97a73bb51e115aa89c17ffa8dd167, 'oUSDT', 6)
    , (0x19aac5f612f524b754ca7e7c41cbfa2e981a4432, 'WKLAY', 18)
    , (0x98a8345bb9d3dda9d808ca1c9142a28f6b0430e1, 'WETH', 18)
    , (0x34d21b1e550d73cee41151c77f3c73359527a396, 'oETH', 18)
    , (0x15d9f3ab1982b0e5a415451259994ff40369f584, 'BTCB', 18)
    , (0x608792deb376cce1c9fa4d0e6b7b44f507cffa6a, 'USDC', 6)
    , (0x754288077d0ff82af7a5317c7cb8c444d421d103, 'oUSDC', 6)
    , (0x574e9c26bda8b95d7329505b4657103710eb32ea, 'oBNB', 18)
    , (0x9eaefb09fe4aabfbe6b1ca316a3c36afc83a393f, 'oXRP', 6)
    , (0xfe41102f325deaa9f303fdd9484eb5911a7ba557, 'oORC-S', 18)
    , (0x210bc03f49052169d5588a52c317f71cf2078b85, 'oBUSD', 18)
    , (0x02cbe46fb8a1f579254a9b485788f2d86cad51aa, 'BORA', 18)
    --below 14 from: https://coinpaprika.com/tag/klaytn-token/home-overview/?all=true
    , (0xd068c52d81f4409b9502da926ace3301cc41f623, 'MBX', 18)
    , (0x17d2628d30f8e9e966c9ba831c9b9b01ea8ea75c, 'ISK', 18)
    , (0xc6a2ad8cc6e4a7e08fc37cc5954be07d499e7654, 'KSP', 18)
    , (0x275f942985503d8ce9558f8377cc526a3aba3566, 'WIKEN', 18)
    , (0x089ebd525949ee505a48eb14eecba653bc8d1b97, 'DICE', 18)
    , (0xe06b40df899b9717b4e6b50711e1dc72d08184cf, 'HIBS', 18)
    , (0x342633c4a7f91094096e15b513f039af52e960d9, 'SELO', 8)
    , (0xc4407f7dc4b37275c9ce0f839652b393e13ff3d1, 'CLBK', 18)
    , (0x3eef2b8d8050197e409b69f09462f49eab562979, 'JUM', 18)
    , (0x4b734a4d5bf19d89456ab975dfb75f02762dda1d, 'MOOI', 18)
    , (0xe950bdcfa4d1e45472e76cf967db93dbfc51ba3e, 'KAI', 18)
    , (0x02518a2a6af8133b59f69a8c162f112f76bb0d96, 'GRAM', 18)
    , (0x656f86dd0f3bc25af2d15855f2a2f142f9eaed87, 'BOX', 18)
    , (0xab9cb20a28f97e189ca0b666b8087803ad636b3c, 'MDUS', 18)
) AS temp_table (contract_address, symbol, decimals)