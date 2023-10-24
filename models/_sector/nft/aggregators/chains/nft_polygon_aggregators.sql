{{config(
    
    schema = 'nft_polygon',
    alias = 'aggregators'
)}}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (0xb3e808e102ac4be070ee3daac70672ffc7c1adca, 'Element') -- Element NFT Marketplace Aggregator
      , (0x84efdf0052bf79f2cd3a2369a5d62322923512af, 'Element') -- Element Swap 2
      , (0x5e06c349a4a1b8dde8da31e0f167d1cb1d99967c, 'Dew') -- Dew
      , (0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad, 'Uniswap') -- Uniswap's Universal Router 3
      , (0xfbf4c42bb3981e6d5b85ad340d7f0213db7b132c, 'BitKeep') -- BitKeep
      , (0x954dab8830ad2b9c312bb87ace96f6cce0f51e3a, 'OKX')  -- OKX
      , (0xc2c862322e9c97d6244a3506655da95f05246fd8, 'Reservoir') -- Reservoir v6.0.1
  ) AS temp_table (contract_address, name)
