{{ config( alias = alias('native'), tags=['static'])}}

SELECT chain, symbol, price_symbol, LOWER(price_address) as price_address, decimals
FROM (VALUES
         ('ethereum', 'ETH', 'WETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 18)
         , ('optimism', 'ETH', 'WETH', '0x4200000000000000000000000000000000000006', 18)
         , ('polygon', 'MATIC', 'MATIC', '0x0000000000000000000000000000000000001010', 18)
         , ('arbitrum', 'ETH', 'WETH', '0x82af49447d8a07e3bd95bd0d56f35241523fbab1', 18)
         , ('avalanche_c', 'AVAX', 'WAVAX', '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 18)
         , ('gnosis', 'xDAI', 'WXDAI', '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d', 18)
         , ('bnb', 'BNB', 'WBNB', '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c', 8)
         , ('fantom', 'FTM', 'WFTM', '0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83', 18)
         , ('solana', 'SOL', 'SOL', 'so11111111111111111111111111111111111111112', 18) --not sure if solana decimals are correct here
     ) AS temp_table (chain, symbol, price_symbol, price_address, decimals)