{{ 
    config(
        schema = 'tokens_zksync'
        , alias = 'stablecoins'
        , tags = ['static']
        , materialized = 'table'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (
    VALUES
        (0x1d17CBcF0D6D143135aE902365D2E5e2A16538D4,    'USDC', 6,  'USD Coin') --native
		, (0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4,  'USDC', 6,  'USD Coin') --bridged
		, (0x4B9eb6c0b6ea15176BBF62841C6B2A8a398cb656,  'DAI',  18, 'Dai')
		, (0x503234F203fC7Eb888EEC8513210612a43Cf6115,  'LUSD',  18, 'LUSD')
) AS temp_table (contract_address, symbol, decimals, name)
