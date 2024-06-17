{{ 
    config(
        schema = 'tokens_linea'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0x176211869ca2b568f2a7d4ee941e073a821ee1ff,    'USDC', 6,  'USD Coin') --bridged
		, (0xb79dd08ea68a908a97220c76d19a6aa9cbde4376,  'USD+', 6,  'Overnight USD')
		, (0xA219439258ca9da29E9Cc4cE5596924745e12B93,  'USDT',  6, 'Tether')
		, (0x4af15ec2a0bd43db75dd04e62faa3b8ef36b00d5,  'DAI',  18, 'Dai')
		, (0xd2bc272EA0154A93bf00191c8a1DB23E67643EC5,  'USDP',  18, 'Pax Dollar')
		, (0xDD3B8084AF79B9BaE3D1b668c0De08CCC2C9429A,  'MIM',  18, 'Magic Internet Money')
) AS temp_table (contract_address, symbol, decimals, name)
