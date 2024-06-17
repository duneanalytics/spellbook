{{ 
    config(
        schema = 'tokens_celo'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0xceba9300f2b948710d2653dd7b07f33a8b32118c,    'USDC', 6,  'USD Coin') --native
        , (0xef4229c8c3250c675f21bcefa42f58efbff6002a,    'USDC', 6,  'USD Coin') --bridged
        , (0x37f750b7cc259a2f741af45294f6a16572cf5cad,    'USDC', 6,  'USD Coin') --bridged
		, (0x48065fbBE25f71C9282ddf5e1cD6D6A887483D5e,  'USDT', 6,  'Tether') --native
		, (0x617f3112bf5397d0467d315cc709ef968d9ba546,  'USDT', 6,  'Tether') --bridged
		, (0x765de816845861e75a25fca122bb6898b8b1282a,  'cUSD',  18, 'Celo Dollar')
) AS temp_table (contract_address, symbol, decimals, name)
