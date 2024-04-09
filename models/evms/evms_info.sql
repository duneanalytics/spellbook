{{ config(
        tags = [ 'static'],
        alias = 'info',
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "celo", "base", "goerli", "zksync", "zora", "scroll"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}')
}}

SELECT chain_id, blockchain, name, chain_type, rollup_type, native_token_symbol, wrapped_native_token_address, explorer_link, first_block_time, codebase, data_availability
FROM (VALUES
        (1, 'ethereum', 'Ethereum', 'Layer 1', NULL, 'ETH', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'https://etherscan.io/', timestamp '2015-07-30 15:26', NULL, NULL)
        , (43114, 'avalanche_c', 'Avalanche C-Chain', 'Layer 1', NULL, 'AVAX', 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7, 'https://snowtrace.io/', timestamp '2020-09-23 11:02', NULL, NULL)
        , (42161, 'arbitrum', 'Arbitrum One', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x82af49447d8a07e3bd95bd0d56f35241523fbab1, 'https://arbiscan.io/', timestamp '2021-05-29 00:35', 'Arbitrum', 'Ethereum Blobs')
        , (10, 'optimism', 'OP Mainnet', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://explorer.optimism.io/', timestamp '2021-11-11 21:16', 'OP Stack', 'Ethereum Blobs')
        , (100, 'gnosis', 'Gnosis', 'Layer 1', NULL, 'xDAI', 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d, 'https://gnosisscan.io/', timestamp '2018-10-08 18:43', NULL, NULL)
        , (137, 'polygon', 'Polygon PoS', 'Layer 1', NULL, 'MATIC', 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270, 'https://polygonscan.com/', timestamp '2020-05-30 16:30', NULL, NULL)
        , (250, 'fantom', 'Fantom', 'Layer 1', NULL, 'FTM', 0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83, 'https://ftmscan.com/', timestamp '2019-12-27 03:56', NULL, NULL)
        , (56, 'bnb', 'BNB', 'Layer 1', NULL, 'BNB', 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c, 'https://bscscan.com/', timestamp '2020-08-29 03:24', NULL, NULL)
        , (5, 'goerli', 'Goerli', 'Testnet', NULL, 'GTH', 0xB4FBF271143F4FBf7B91A5ded31805e42b2208d6, 'https://goerli.etherscan.io/', timestamp '2015-07-30 03:26', NULL, NULL)
        , (42220, 'celo', 'Celo', 'Layer 1', NULL, 'CELO', 0x471EcE3750Da237f93B8E339c536989b8978a438, 'https://celoscan.io/', timestamp '2020-04-22 16:00', NULL, NULL)
        , (8453, 'base', 'Base', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://basescan.org/', timestamp '2023-06-15 00:35', 'OP Stack', 'Ethereum Blobs')
        , (7777777, 'zora', 'ZORA', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://explorer.zora.energy/', timestamp '2023-06-14 00:03', 'OP Stack', 'Ethereum Blobs')
        , (534352, 'scroll', 'Scroll', 'Layer 2', 'ZK Rollup', 'ETH', 0x5300000000000000000000000000000000000004, 'https://scrollscan.com/', timestamp '2023-10-10 06:00', 'Scroll', 'Ethereum Blobs')
        , (424, 'pgn', 'Public Goods Network', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://explorer.publicgoods.network/', timestamp '2023-07-11 22:18', 'OP Stack', 'Ethereum')
        , (324, 'zksync', 'zkSync Era', 'Layer 2', 'ZK Rollup', 'ETH', 0x5aea5775959fbc2557cc8789bc1bf90a239d9a91, 'https://explorer.zksync.io/', timestamp '2023-02-14 15:22', 'zkSync', 'Ethereum Blobs')
        , (1101, 'zkevm', 'Polygon zkEVM', 'Layer 2', 'ZK Rollup', 'ETH', 0x4f9a0e7fd2bf6067db6994cf12e4495df938e6e9, 'https://zkevm.polygonscan.com/', timestamp '2023-03-24 05:30', 'Polygon', 'Ethereum')
        , (1088, 'metis_andromeda', 'Metis Andromeda', NULL, NULL, NULL, NULL, 'https://andromeda-explorer.metis.io/', timestamp '2021-11-18 22:19', 'Optimistic Virtual Machine', 'Ethereum')
        , (5000, 'mantle', 'Mantle', 'Layer 2', 'Optimistic Rollup', 'MNT', NULL, 'https://explorer.mantle.xyz/', timestamp '2023-07-02 18:21', 'Optimistic Virtual Machine', 'Ethereum')
        , (59144, 'linea', 'Linea', 'Layer 2', 'ZK Rollup', 'ETH', 0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f, 'https://lineascan.build/', timestamp '2023-07-06 13:15', NULL, 'Ethereum Blobs')
        , (42170, 'arbitrum_nova', 'Arbitrum Nova', 'Layer 2', 'Optimistic Rollup', 'ETH', NULL, 'https://nova-explorer.arbitrum.io/', timestamp '2022-06-25 04:01', 'Arbitrum', 'Ethereum')
        , (288, 'boba', 'Boba Network', 'Layer 2', 'Optimistic Rollup', 'ETH', NULL, 'https://bobascan.com/', timestamp '2021-10-28 05:03', 'Optimistic Virtual Machine', 'Ethereum')
        , (7700, 'canto', 'Canto', 'Layer 2', NULL, 'ETH', NULL, 'https://evm.explorer.canto.io/', timestamp '2022-07-26 19:27', NULL, 'Ethereum')
        , (420, 'optimism_goerli', 'Optimism Goerli', 'Testnet', 'Optimistic Rollup', 'GTH', 0x4200000000000000000000000000000000000006, 'https://optimism-goerli.blockscout.com/', timestamp '2022-06-09 16:55', 'OP Stack', 'goerli')
        , (1313161554, 'aurora', 'Aurora', 'Layer 2', NULL, 'ETH', 0xC9BdeEd33CD01541e1eeD10f90519d2C06Fe3feB, 'https://explorer.aurora.dev/', timestamp '2020-07-21 21:50:11', NULL, NULL)
        , (8217, 'klaytn', 'Klaytn', 'Layer 1', NULL, 'KLAY', 0xe4f05a66ec68b54a58b17c22107b02e0232cc817, 'https://scope.klaytn.com/', timestamp '2019-06-25 13:41:14', NULL, NULL)
        , (34443, 'mode', 'Mode', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://explorer.mode.network/', timestamp '2023-11-16 20:46:23', 'OP Stack', 'Ethereum Blobs')
        , (291, 'orderly', 'Orderly Network', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://explorer.orderly.network/', timestamp '2023-10-06 16:03:49', 'OP Stack', 'Ethereum')
        , (957, 'lyra', 'Lyra', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://explorer.lyra.finance/', cast(NULL AS timestamp), 'OP Stack', 'Ethereum')
        , (169, 'manta_pacific', 'Manta Pacific', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://pacific-explorer.manta.network/', timestamp '2023-09-09 02:45:59', 'OP Stack', 'Celestia')
        , (204, 'opbnb', 'opBNB', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://opbnbscan.com/', timestamp '2023-08-11 11:35:24', 'OP Stack', 'BNB')
        , (255, 'kroma', 'Kroma', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://blockscout.kroma.network/', timestamp '2023-09-04 22:19:49', 'OP Stack', 'Ethereum')
        , (116, 'debank', 'DeBank', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://explorer.debank.com/', cast(NULL AS timestamp), 'OP Stack', NULL)
        , (570, 'rollux', 'Rollux', 'Layer 2', 'Optimistic Rollup', 'ETH', 0x4200000000000000000000000000000000000006, 'https://explorer.rollux.com/', timestamp '2023-06-21 07:34:01', 'OP Stack', 'Bitcoin')
        , (666666666, 'degen', 'DEGEN', 'Layer 3', 'Optimistic Rollup', 'DEGEN', NULL, 'https://explorer.degen.tips/', timestamp '2024-03-10 17:18:59', 'Arbitrum Orbit', 'AnyTrust')
        , (1750, 'metal', 'Metal', 'Layer 2', 'Optimistic Rollup', 'ETH', NULL, 'https://explorer.metall2.com/', timestamp '2024-03-27 19:18:35', 'OP Stack', 'Ethereum Blobs')
        , (81457, 'blast', 'Blast', 'Layer 2', 'Optimistic Rollup', 'ETH', NULL, 'https://blastscan.io/', timestamp '2024-02-24 09:23:35', NULL, 'Ethereum')
        , (168587773, 'blast_sepolia', 'Blast Sepolia', 'Testnet Layer 2', 'Optimistic Rollup', 'ETH', NULL, 'https://testnet.blastscan.io/', timestamp '2024-01-08 04:04:48', NULL, 'Sepolia')
        ) AS temp_table (chain_id, blockchain, name, chain_type, rollup_type, native_token_symbol, wrapped_native_token_address, explorer_link, first_block_time, codebase, data_availability)
