 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      ('0x0a267cf51ef038fc00e71801f5a524aec06e4f07', 'Genie') -- Genie
    , ('0x2af4b707e1dce8fc345f38cfeeaa2421e54976d5', 'Genie') -- Genie 2
    , ('0xf24629fbb477e10f2cf331c2b7452d8596b5c7a5', 'Gem') -- Gem
    , ('0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2', 'Gem') -- Gem 2
    , ('0x0000000031f7382a812c64b604da4fc520afef4b', 'Gem') -- Gem Single Contract Checkout 1
    , ('0x0000000035634b55f3d99b071b5a354f48e10bef', 'Gem') -- Gem Single Contract Checkout 2
    , ('0x00000000a50bb64b4bbeceb18715748dface08af', 'Gem') -- Gem Single Contract Checkout 3
    , ('0xae9c73fd0fd237c1c6f66fe009d24ce969e98704', 'Gem') -- Gem Protection Enabled Address
    , ('0x56dd5bbede9bfdb10a2845c4d70d4a2950163044', 'X2Y2') -- X2Y2's OpenSea Sniper
    , ('0x69cf8871f61fb03f540bc519dd1f1d4682ea0bf6', 'Element') -- Element NFT Marketplace Aggregator
  ) AS temp_table (contract_address, name)