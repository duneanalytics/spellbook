{{ config(
        schema='prices',
        alias = 'trusted_tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}


-- Generated from https://dune.com/queries/3355223
SELECT blockchain AS blockchain
     , symbol AS symbol
     , contract_address AS contract_address
FROM (VALUES ('arbitrum', 'USDC.e', 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8),
             ('arbitrum', 'WETH', 0x82af49447d8a07e3bd95bd0d56f35241523fbab1),
             ('arbitrum', 'USDC', 0xaf88d065e77c8cc2239327c5edb3a432268e5831),
             ('arbitrum', 'USDT', 0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9),
             ('arbitrum', 'ARB', 0x912ce59144191c1204e64559fe8253a0e49e6548),
             ('arbitrum', 'WBTC', 0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f),
             ('arbitrum', 'DAI', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1),
             ('avalanche_c', 'WAVAX', 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7),
             ('avalanche_c', 'USDC', 0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e),
             ('avalanche_c', 'USDt', 0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7),
             ('avalanche_c', 'USDT.e', 0xc7198437980c041c805a1edcba50c1ce5db95118),
             ('avalanche_c', 'BTC.b', 0x152b9d0fdc40c096757f570a51e494bd4b943e50),
             ('avalanche_c', 'WETH.e', 0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab),
             ('avalanche_c', 'DAI.e', 0xd586e7f844cea2f87f50152665bcbc2c279d8d70),
             ('base', 'USDbC', 0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca),
             ('base', 'USDC', 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913),
             ('base', 'WETH', 0x4200000000000000000000000000000000000006),
             ('base', 'DAI', 0x50c5725949a6f0c72e6c4a641f24049a917db0cb),
             ('base', 'axlUSDC', 0xeb466342c4d449bc9f53a865d5cb90586f405215),
             ('base', 'cbETH', 0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22),
             ('base', 'wstETH', 0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452),
             ('bnb', 'USDT', 0x55d398326f99059ff775485246999027b3197955),
             ('bnb', 'WBNB', 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c),
             ('bnb', 'BUSD', 0xe9e7cea3dedca5984780bafc599bd69add087d56),
             ('bnb', 'USDC', 0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d),
             ('bnb', 'ETH', 0x2170ed0880ac9a755fd29b2688956bd959f933f8),
             ('bnb', 'BTCB', 0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c),
             ('bnb', 'DAI', 0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3),
             ('celo', 'cUSD', 0x765de816845861e75a25fca122bb6898b8b1282a),
             ('celo', 'CELO', 0x471ece3750da237f93b8e339c536989b8978a438),
             ('celo', 'cEUR', 0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73),
             ('celo', 'BTC', 0xd629eb00deced2a080b7ec630ef6ac117e614f1b),
             ('celo', 'BIFI', 0x639a647fbe20b6c8ac19e48e2de44ea792c62c5c),
             ('ethereum', 'WETH', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2),
             ('ethereum', 'USDT', 0xdac17f958d2ee523a2206206994597c13d831ec7),
             ('ethereum', 'USDC', 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
             ('ethereum', 'DAI', 0x6b175474e89094c44da98b954eedeac495271d0f),
             ('ethereum', 'WBTC', 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599),
             ('ethereum', 'crvUSD', 0xf939e0a03fb07f59a73314e73794be0e57ac1b4e),
             ('fantom', 'WFTM', 0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83),
             ('fantom', 'USDC', 0x04068da6c83afcfa0e13ba15a6696662335d5b75),
             ('fantom', 'WETH', 0x74b23882a30290451a17c44f4f05243b6b58c76d),
             ('fantom', 'DAI', 0x8d11ec38a3eb5e956b052f67da8bdc9bef8abf3e),
             ('fantom', 'WBTC', 0x321162cd933e2be498cd2267a90534a804051b11),
             ('gnosis', 'WXDAI', 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d),
             ('gnosis', 'WETH', 0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1),
             ('gnosis', 'USDC', 0xddafbb505ad214d7b80b1f830fccc89b60fb7a83),
             ('gnosis', 'GNO', 0x9c58bacc331c9aa871afd802db6379a98e80cedb),
             ('gnosis', 'USDT', 0x4ecaba5870353805a9f068101a40e0f32ed605c6),
             ('gnosis', 'WBTC', 0x8e5bbbb09ed1ebde8674cda39a0c169401db4252),
             ('gnosis', 'DAI', 0x44fa8e6f47987339850636f88629646662444217),
             ('gnosis', 'BUSD', 0xdd96b45877d0e8361a4ddb732da741e97f3191ff),
             ('optimism', 'USDC.e', 0x7f5c764cbc14f9669b88837ca1490cca17c31607),
             ('optimism', 'WETH', 0x4200000000000000000000000000000000000006),
             ('optimism', 'USDT', 0x94b008aa00579c1307b0ef2c499ad98a8ce58e58),
             ('optimism', 'USDC', 0x0b2c639c533813f4aa9d7837caf62653d097ff85),
             ('optimism', 'DAI', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1),
             ('optimism', 'OP', 0x4200000000000000000000000000000000000042),
             ('optimism', 'WBTC', 0x68f180fcce6836688e9084f035309e29bf0a2095),
             ('optimism', 'sUSD', 0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9),
             ('optimism', 'wstETH', 0x1f32b1c2345538c0c6f582fcb022739c4a194ebb),
             ('polygon', 'USDC.e', 0x2791bca1f2de4661ed88a30c99a7a9449aa84174),
             ('polygon', 'USDT', 0xc2132d05d31c914a87c6611c10748aeb04b58e8f),
             ('polygon', 'WMATIC', 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270),
             ('polygon', 'WETH', 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619),
             ('polygon', 'DAI', 0x8f3cf7ad23cd3cadbd9735aff958023239c6a063),
             ('polygon', 'WBTC', 0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6),
             ('zksync', 'USDC.e', 0x3355df6d4c9c3035724fd0e3914de96a5a83aaf4),
             ('zksync', 'BUSD', 0x2039bb4116b4efc145ec4f0e2ea75012d6c0f181),
             ('zksync', 'WETH', 0x5aea5775959fbc2557cc8789bc1bf90a239d9a91),
             ('zksync', 'USDT.e', 0x493257fd37edb34451f62edf8d2a0c418852ba4c),
             ('zksync', 'ETH', 0x000000000000000000000000000000000000800a),
             ('zksync', 'WBTC', 0xbbeb516fb02a01611cbbe0453fe3c580d7281011),
             ('zora', 'WETH', 0x4200000000000000000000000000000000000006)
) AS t (blockchain, symbol, contract_address)