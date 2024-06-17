{{ 
    config(
        schema = 'tokens_base'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913,    'USDC', 6,  'USD Coin')
		, (0xb79dd08ea68a908a97220c76d19a6aa9cbde4376,  'USD+', 6,  'USD+')
		, (0x50c5725949a6f0c72e6c4a641f24049a917db0cb,  'DAI',  18, 'Dai')
		, (0x4a3a6dd60a34bb2aba60d73b4c88315e9ceb6a3d,  'MIM',  18, 'Magic Internet Money')
) AS temp_table (contract_address, symbol, decimals, name)
