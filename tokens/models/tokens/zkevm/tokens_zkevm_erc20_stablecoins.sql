{{ 
    config(
        schema = 'tokens_zkevm'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035,    'USDC', 6,  'USD Coin')
        , (0x37eaa0ef3549a5bb7d431be78a3d99bd360d19e5,  'USDC', 6,  'USD Coin')
        , (0x1e4a5963abfd975d8c9021ce480b42188849d41d,  'USDT', 6,  'Tether')
        , (0xc5015b9d9161dca7e18e32f6f25c4ad850731fd4,  'DAI',  18, 'Dai')
        , (0xFf8544feD5379D9ffa8D47a74cE6b91e632AC44D,  'FRAX',  18, 'Frax')
) AS temp_table (contract_address, symbol, decimals, name)
