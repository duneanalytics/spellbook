{{ 
    config(
        schema = 'tokens_polygon'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359,    'USDC',     6,  'USD Coin') --native 
        , (0x2791bca1f2de4661ed88a30c99a7a9449aa84174,  'USDC',     6,  'USD Coin') --bridged 
        , (0xc2132d05d31c914a87c6611c10748aeb04b58e8f,  'USDT',     6,  'Tether') --unkknown
        , (0x8f3cf7ad23cd3cadbd9735aff958023239c6a063,  'DAI',      18, 'Dai') --unkknown
        , (0x45c32fa6df82ead1e2ef74d17b76547eddfaff89,  'FRAX',     18, 'Frax') --unkknown
        , (0xdab529f40e671a1d4bf91361c21bf9f0c9712ab7,  'BUSD',     18, 'Binance USD') --unkknown
        , (0xc4Ce1D6F5D98D65eE25Cf85e9F2E9DcFEe6Cb5d6,  'crvUSD',   18, 'Curve USD') --unkknown
        , (0x2e1ad108ff1d8c782fcbbb89aad783ac49586756,  'TUSD',     18, 'True USD') --unkknown
        , (0x236eec6359fb44cce8f97e99387aa7f8cd5cde1f,  'USD+',     6,  'Overnight USD') --unkknown
) AS temp_table (contract_address, symbol, decimals, name)
