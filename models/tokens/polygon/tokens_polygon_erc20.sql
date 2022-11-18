{{ config( alias='erc20')}}

SELECT LOWER(contract_address) AS contract_address, symbol, decimals
  FROM (VALUES 

('0x8f3cf7ad23cd3cadbd9735aff958023239c6a063', 'DAI', 18)
,('0x0000000000000000000000000000000000001010', 'MATIC', 18)
,('0x2791bca1f2de4661ed88a30c99a7a9449aa84174', 'USDC', 6)
,('0xc2132d05d31c914a87c6611c10748aeb04b58e8f', 'USDT', 6)
,('0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6', 'WBTC', 8)
,('0x7ceb23fd6bc0add59e62ac25578270cff1b9f619', 'WETH', 18)
,('0x2c89bbc92bd86f8075d1decc58c7f4e0107f286b', 'WAVAX', 18)
        
) AS temp_table (contract_address, symbol, decimals)
