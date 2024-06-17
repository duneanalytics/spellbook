{{ 
    config(
        schema = 'tokens_blast'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0x4300000000000000000000000000000000000003,    'USDB', 18,  'Blast USD')
		, (0x76da31d7c9cbeae102aff34d3398bc450c8374c1,  'MIM',  18, 'Magic Internet Money')
) AS temp_table (contract_address, symbol, decimals, name)
