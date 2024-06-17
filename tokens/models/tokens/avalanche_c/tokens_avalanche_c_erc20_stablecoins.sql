{{ 
    config(
        schema = 'tokens_avalance_c'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E,    'USDC', 6,  'USD Coin')
		, (0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664,  'USDC', 6,  'USC Coin') --bridged
		, (0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7,  'USDT', 6,  'Tether')
) AS temp_table (contract_address, symbol, decimals, name)
