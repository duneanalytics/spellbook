{{config(
    tags = ['dunesql'],
    alias='aggregators_manual'
)}}
SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (0x0a267cf51ef038fc00e71801f5a524aec06e4f07, 'Genie') -- Genie
    , (0x2af4b707e1dce8fc345f38cfeeaa2421e54976d5, 'Genie') -- Genie 2
    , (0xcdface5643b90ca4b3160dd2b5de80c1bf1cb088, 'Genie') -- Genie
    , (0x31837aaf36961274a04b915697fdfca1af31a0c7, 'Genie') -- Genie
    , (0xf97e9727d8e7db7aa8f006d1742d107cf9411412, 'Genie') -- Genie
    , (0xf24629fbb477e10f2cf331c2b7452d8596b5c7a5, 'Gem') -- Gem
    , (0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2, 'Gem') -- Gem 2
    , (0x0000000031f7382a812c64b604da4fc520afef4b, 'Gem') -- Gem Single Contract Checkout 1
    , (0x0000000035634b55f3d99b071b5a354f48e10bef, 'Gem') -- Gem Single Contract Checkout 2
    , (0x00000000a50bb64b4bbeceb18715748dface08af, 'Gem') -- Gem Single Contract Checkout 3
    , (0xae9c73fd0fd237c1c6f66fe009d24ce969e98704, 'Gem') -- Gem Protection Enabled Address
    , (0x9e97195f937c9372fe5fda5e3b86e9b88cbefed7, 'Gem') -- Gem's X2Y2 Batch Buys (Old)
    , (0x539ea5d6ec0093ff6401dbcd14d049c37a77151b, 'Gem') -- Gem's X2Y2 Batch Buys
    , (0xf22007700b8c443bcb36a39580f7804bffdb1169, 'Gem') -- Gem's Blur Batch Buys
    , (0x4326275317acc0fae4aa5c68fce4c54c74dc08d3, 'Gem') -- Gem v2 X2Y2
    , (0x29ab6d8f7e3d815168d6b40ebb12625b4fe13998, 'Gem') -- Gem v2
    , (0x241b8e59e81455e66b9cd0e2ffb2506be1838144, 'Gem') -- Gem v2
    , (0x56dd5bbede9bfdb10a2845c4d70d4a2950163044, 'X2Y2') -- X2Y2's OpenSea Sniper
    , (0x69cf8871f61fb03f540bc519dd1f1d4682ea0bf6, 'Element') -- Element NFT Marketplace Aggregator
    , (0xb4e7b8946fa2b35912cc0581772cccd69a33000c, 'Element') -- Element NFT Marketplace Aggregator 2
    , (0x39da41747a83aee658334415666f3ef92dd0d541, 'Blur') -- Blur
    , (0x7f6cdf5869bd780ea351df4d841f68d73cbcc16b, 'NFTInit') -- NFTInit.com
    , (0x92701d42e1504ef9fce6d66a2054218b048dda43, 'OKX') -- OKX
    , (0xc52b521b284792498c1036d4c2ed4b73387b3859, 'Reservoir') -- Reservoir v1
    , (0x5aa9ca240174a54af6d9bfc69214b2ed948de86d, 'Reservoir') -- Reservoir v2
    , (0x7c9733b19e14f37aca367fbd78922c098c55c874, 'Reservoir') -- Reservoir v3
    , (0x8005488ff4f8982d2d8c1d602e6d747b1428dd41, 'Reservoir') -- Reservoir v4
    , (0x9ebfb53fa8526906738856848a27cb11b0285c3f, 'Reservoir') -- Reservoir v5
    , (0x178a86d36d89c7fdebea90b739605da7b131ff6a, 'Reservoir') -- Reservoir v6
    , (0x39b6862c4783db2651d64bc160349dc9a15f1fb7, 'Rarity Garden') -- Rarity Garden v2
    , (0x9d0a89bc35fb160a076de0341d9280830d3013ca, 'Rarity Garden') -- Rarity Garden v1.02
    , (0x603d022611bfe6a101dcdab207d96c527f1d4d8e, 'BitKeep') -- BitKeep
    , (0x2a7251d1e7d708c507b1b0d3ff328007beecce5d, 'Rarible') -- Rarible
    , (0x2c45af926d5f62c5935278106800a03eb565778e, 'Rarible') -- Rarible
    , (0x1ee3151cff01321059e3865214379b85c79ca984, 'Magic Eden') -- Magic Eden
    , (0x141efc30c4093bc0f8204accb8afa6643fddecf2, 'Alpha Sharks') -- Alpha Sharks
    , (0x552b16d19dbad7af2786fe5a40d96d2a5c09428c, 'Alpha Sharks') -- Alpha Sharks 2.0
    , (0x114e54a100a0415abf9727234c92c83dbcc59abf, 'Alpha Sharks') -- Alpha Sharks 2.1
    , (0x1ed3d33b41e392014e0c9d8125369aba4e09798f, 'Alpha Sharks') -- Magically (Alpha Sharks)
    , (0x04898894a0b6c094a920eafd180ef4ac30f00a43, 'Alpha Sharks') -- Magically (Alpha Sharks)
    , (0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b, 'Uniswap') -- Uniswap's Universal Router
    , (0x4c60051384bd2d3c01bfc845cf5f4b44bcbe9de5, 'Uniswap') -- Uniswap's Universal Router (New)
    , (0x00000000005228b791a99a61f36a130d50600106, 'LooksRare') -- LooksRare Aggregator
    , (0x36ab1c395b3711d3d5ed2af8ac8371cc991aa06c, 'Flip') -- Flip
    , (0x7db11e30ae8ad7495668701c3f2c1b6d60587eda, 'Flip') -- Flip's LooksRare Checkout
    , (0xb123504fa220ba482768dd1e798594c1af88d7dc, 'Tiny Astro') -- Tiny Astro
    , (0x4c9712cd94376c537464caa4d87bce198d59936c, 'GigaMart') -- GigaMart's GigaAggregator
    , (0xdc077a4e3f46138dedac5d684882d33fcc927cf7, 'GigaMart') -- GigaMart's GigaAggregator v 1.2
    , (0x4abab19c2ad968adbc52e2c8a5ccda5379629576, 'Skillet') -- Skillet
  ) AS temp_table (contract_address, name)
