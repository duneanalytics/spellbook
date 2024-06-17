{{ 
    config(
        schema = 'tokens_scroll'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4,    'USDC', 6,  'USD Coin')
		, (0xf55bec9cafdbe8730f096aa55dad6d22d44099df,  'USDT', 6,  'Tether')
		, (0xca77eb3fefe3725dc33bccb54edefc3d9f764f97,  'DAI',  18, 'Dai')
		, (0xedeabc3a1e7d21fe835ffa6f83a710c70bb1a051,  'LUSD', 18, 'Liquidity USD')
) AS temp_table (contract_address, symbol, decimals, name)
