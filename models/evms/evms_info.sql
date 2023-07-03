{{ config(
        tags = ['dunesql', 'static'],
        alias = alias('info', timestamp ),
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\', timestamp ) }}'
        )
}}

SELECT chain_id, blockchain, name, native_token_symbol, wrapped_native_token_address, explorer_link, first_block_time
FROM (VALUES
        (1, 'ethereum', 'Ethereum', 'ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 'https://etherscan.io', timestamp '2015-07-30 15:26')
        , (43114, 'avalanche_c', 'Avalanche', 'AVAX', '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'https://optimistic.etherscan.io', timestamp '2020-09-23 11:02')
        , (42161, 'arbitrum', 'Arbitrum', 'ETH', '0x82af49447d8a07e3bd95bd0d56f35241523fbab1', 'https://arbiscan.io', timestamp '2021-05-29 00:35')
        , (10, 'optimism', 'Optimism', 'ETH', '0x4200000000000000000000000000000000000006', 'https://optimistic.etherscan.io', timestamp '2021-11-11 21:16')
        , (100, 'gnosis', 'Gnosis', 'xDAI', '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d', 'https://gnosisscan.io', timestamp '2018-10-08 18:43')
        , (137, 'polygon', 'Polygon', 'MATIC', '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270', 'https://polygonscan.com', timestamp '2020-05-30 16:30')
        , (250, 'fantom', 'Fantom', 'FTM', '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83', 'https://ftmscan.com', timestamp '2019-12-27 03:56')
        , (56, 'bnb', 'BNB', 'BNB', '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c', 'https://bscscan.com', timestamp '2020-08-29 03:24')
        , (42220, 'celo', 'Celo', NULL, 'https://celoscan.io/', timestamp '2020-04-22 16:00')
        ) AS temp_table (chain_id, blockchain, name, native_token_symbol, wrapped_native_token_address, first_block_time)

