{{ 
    config(
        schema = 'tokens_tron'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0xa614f803b6fd780986a42c78ec9c7f77e6ded13c,    'USDT', 6,  'Tether')
		, (0x94f24e992ca04b49c6f2a2753076ef8938ed4daa,  'USDD', 18, 'Decentralized USD')
		, (0x3487b63d30b5b2c87fb7ffa8bcfade38eaac1abe,  'USDC', 6,  'USD Coin')
		, (0xcebde71077b830b958c8da17bcddeeb85d0bcf25,  'TUSD', 18, 'TrueUSD')
		, (0x83c91bfde3e6d130e286a3722f171ae49fb25047,  'BUSD', 18, 'Binance USD')
) AS temp_table (contract_address, symbol, decimals, name)
