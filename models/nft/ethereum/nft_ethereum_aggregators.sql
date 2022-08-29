 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      ('0x0a267cf51ef038fc00e71801f5a524aec06e4f07', 'Genie') -- Genie
    , ('0x2af4b707e1dce8fc345f38cfeeaa2421e54976d5', 'Genie') -- Genie 2
    , ('0xcdface5643b90ca4b3160dd2b5de80c1bf1cb088', 'Genie') -- Genie
    , ('0x31837aaf36961274a04b915697fdfca1af31a0c7', 'Genie') -- Genie
    , ('0xf97e9727d8e7db7aa8f006d1742d107cf9411412', 'Genie') -- Genie
    , ('0xf24629fbb477e10f2cf331c2b7452d8596b5c7a5', 'Gem') -- Gem
    , ('0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2', 'Gem') -- Gem 2
    , ('0x0000000031f7382a812c64b604da4fc520afef4b', 'Gem') -- Gem Single Contract Checkout 1
    , ('0x0000000035634b55f3d99b071b5a354f48e10bef', 'Gem') -- Gem Single Contract Checkout 2
    , ('0x00000000a50bb64b4bbeceb18715748dface08af', 'Gem') -- Gem Single Contract Checkout 3
    , ('0xae9c73fd0fd237c1c6f66fe009d24ce969e98704', 'Gem') -- Gem Protection Enabled Address
    , ('0x56dd5bbede9bfdb10a2845c4d70d4a2950163044', 'X2Y2') -- X2Y2's OpenSea Sniper
    , ('0x69cf8871f61fb03f540bc519dd1f1d4682ea0bf6', 'Element') -- Element NFT Marketplace Aggregator
    , ('0x39da41747a83aee658334415666f3ef92dd0d541', 'Blur') -- Blur
    , ('0x7f6cdf5869bd780ea351df4d841f68d73cbcc16b', 'NFTInit') -- NFTInit.com
    , ('0x92701d42e1504ef9fce6d66a2054218b048dda43', 'OKX') -- OKX
    , ('0xc52b521b284792498c1036d4c2ed4b73387b3859', 'Reservoir') -- Reservoir v1
    , ('0x5aa9ca240174a54af6d9bfc69214b2ed948de86d', 'Reservoir') -- Reservoir v2
    , ('0x7c9733b19e14f37aca367fbd78922c098c55c874', 'Reservoir') -- Reservoir v3
    , ('0x8005488ff4f8982d2d8c1d602e6d747b1428dd41', 'Reservoir') -- Reservoir v4
    , ('0x9ebfb53fa8526906738856848a27cb11b0285c3f', 'Reservoir') -- Reservoir v5
    , ('0x39b6862c4783db2651d64bc160349dc9a15f1fb7', 'Rarity Garden') -- Rarity Garden v2
    , ('0x603d022611bfe6a101dcdab207d96c527f1d4d8e', 'BitKeep') -- BitKeep
    , ('0x2a7251d1e7d708c507b1b0d3ff328007beecce5d', 'Rarible') -- Rarible
  ) AS temp_table (contract_address, name)