{{ 
    config(
        schema = 'tokens_arbitrum'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0xaf88d065e77c8cc2239327c5edb3a432268e5831,    'USDC', 6,  'USD Coin') -- Native
		, (0xff970a61a04b1ca14834a43f5de4533ebddb5cc8,  'USDC', 6,  'USD Coin') -- Bridged
		, (0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9,  'USDT', 6,  'Tether')
		, (0xda10009cbd5d07dd0cecc66161fc93d7c9000da1,  'DAI',  18, 'Dai')
		, (0x17fc002b466eec40dae837fc4be5c67993ddbd6f,  'FRAX', 18, 'Frax')
) AS temp_table (contract_address, symbol, decimals, name)
