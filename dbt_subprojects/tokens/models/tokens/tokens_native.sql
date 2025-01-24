{{ config(
        alias='native',
        tags=['static'],
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","optimism", "gnosis", "fantom", "polygon","solana", "celo", "zksync", "mantle","blast","scroll","linea"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xManny","hildobby","soispoke","dot2dotseurat","mtitus6","wuligy","lgingerich","angus_1","Henrystats","rantum"]\') }}')}}

SELECT chain, symbol, price_symbol, price_address, decimals
FROM (VALUES
         ('ethereum', 'ETH', 'WETH', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18)
         , ('optimism', 'ETH', 'WETH', 0x4200000000000000000000000000000000000006, 18)
         , ('base', 'ETH', 'WETH', 0x4200000000000000000000000000000000000006, 18)
         , ('zora', 'ETH', 'WETH', 0x4200000000000000000000000000000000000006, 18)
         , ('polygon', 'MATIC', 'MATIC', 0x0000000000000000000000000000000000001010, 18)
         , ('arbitrum', 'ETH', 'WETH', 0x82af49447d8a07e3bd95bd0d56f35241523fbab1, 18)
         , ('avalanche_c', 'AVAX', 'WAVAX', 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7, 18)
         , ('gnosis', 'xDAI', 'WXDAI', 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d, 18)
         , ('bnb', 'BNB', 'WBNB', 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c, 18)
         , ('fantom', 'FTM', 'WFTM', 0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83, 18)
         , ('solana', 'SOL', 'SOL', from_base58('so11111111111111111111111111111111111111112'), 18) --not sure if solana decimals are correct here
         , ('celo', 'CELO', 'CELO', 0x471ece3750da237f93b8e339c536989b8978a438, 18)
         , ('zksync', 'ETH', 'WETH', 0x000000000000000000000000000000000000800A, 18)
         , ('mantle', 'MNT', 'WMNT', 0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8, 18)
         , ('blast', 'ETH', 'WETH', 0x4300000000000000000000000000000000000004, 18)
         , ('mode', 'MODE', 'MODE', 0xdfc7c877a950e49d2610114102175a06c2e3167a, 18)
         , ('scroll', 'ETH', 'WETH', 0x5300000000000000000000000000000000000004, 18)
         , ('linea', 'ETH', 'WETH', 0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f, 18)
         
         , ('ethereum', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('optimism', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('nova', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('canto', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('sei', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('bnb', 'BNB', 'BNB', 0x0000000000000000000000000000000000000000, 18)
         , ('worldchain', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('bob', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('linea', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('celo', 'CELO', 'CELO', 0x0000000000000000000000000000000000000000, 18)
         , ('boba', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('avalanche_c', 'AVAX', 'AVAX', 0x0000000000000000000000000000000000000000, 18)
         , ('mode', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('polygon', 'POL', 'POL', 0x0000000000000000000000000000000000000000, 18)
         , ('blast', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('zkevm', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('scroll', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('mantle', 'MNT', 'MNT', 0x0000000000000000000000000000000000000000, 18)
         , ('zksync', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('arbitrum', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('b3', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('soneium', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('ink', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('base', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('flare', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('taiko', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('zora', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
         , ('unichain', 'ETH', 'ETH', 0x0000000000000000000000000000000000000000, 18)
     ) AS temp_table (chain, symbol, price_symbol, price_address, decimals)
