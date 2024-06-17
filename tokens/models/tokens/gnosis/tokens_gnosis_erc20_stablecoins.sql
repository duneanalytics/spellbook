{{ 
    config(
        schema = 'tokens_gnosis'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0xddafbb505ad214d7b80b1f830fccc89b60fb7a83,    'USDC', 6,  'USD Coin')
		, (0x4ecaba5870353805a9f068101a40e0f32ed605c6,  'USDT', 6,  'Tether')
		, (0xe91d153e0b41518a2ce8dd3d7944fa863463a97d,  'WXDAI',  18, 'Wrapped xDai')
) AS temp_table (contract_address, symbol, decimals, name)
