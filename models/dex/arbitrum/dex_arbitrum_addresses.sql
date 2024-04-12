{{config(
        schema = 'dex_arbitrum',
        alias = 'addresses',
        tags=['static'])
}}

SELECT blockchain, address, dex_name, distinct_name
FROM (VALUES
     ('arbitrum', 0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45, 'Uniswap', 'SwapRouter02'),
     ('arbitrum', 0x1b02da8cb0d097eb8d57a175b88c7d8b47997506, 'SushiSwap', 'UniswapV2Router02'),
     ('arbitrum', 0xe8c97bf6d084880de38aec1a56d97ed9fdfa0c9b, 'Slingshot Finance', 'Swap'),
     ('arbitrum', 0xabbc5f99639c9b6bcb58544ddf04efa6802f4064, 'GMX', 'Router'),
     ('arbitrum', 0xa27c20a7cf0e1c68c0460706bb674f98f362bc21, 'GMX', 'OrderBookReader'),
     ('arbitrum', 0x3d6ba331e3d9702c5e8a8d254e5d8a285f223aba, 'GMX', 'PositionRouter'),
     ('arbitrum', 0xdd94018f54e565dbfc939f7c44a16e163faab331, 'Odos', 'OdosRouter'),
     ('arbitrum', 0xc30141b657f4216252dc59af2e7cdb9d8792e1b0, 'Socket', 'Registry'),
     ('arbitrum', 0x3b6067d4caa8a14c63fdbe6318f27a0bbc9f9237, 'DODO', 'DODORouteProxy'),
     ('arbitrum', 0xe05dd51e4eb5636f4f0e8e7fbe82ea31a2ecef16, 'DODO', 'DODOFeeRouteProxy'),
     ('arbitrum', 0xdef1c0ded9bec7f1a1670819833240f027b25eff, 'ZeroEx', 'ExchangeProxy'),
     ('arbitrum', 0x5543550d65813c1fa76242227cbba0a28a297771, 'Slingshot Finance', 'Aggregator'),
     ('arbitrum', 0x777777773fdd8b28bb03377d10fcea75ad9768da, 'Via router', 'ViaRouter'),
     ('arbitrum', 0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae, 'LiFi', 'LiFiDiamond_v2'),
     ('arbitrum', 0xc873fecbd354f5a56e00e710b90ef4201db2448d, 'Camelot', 'CamelotRouter'),
     ('arbitrum', 0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad, 'Uniswap', 'UniversalRouter'),
     ('arbitrum', 0xb4315e873dbcf96ffd0acd8ea43f689d8c20fb30, 'Trader Joe', 'LBRouter'),
     ('arbitrum', 0xbee5c10cf6e4f68f831e11c1d9e59b43560b3642, 'Trader Joe', 'V1 Router'),
     ('arbitrum', 0x7bfd7192e76d950832c77bb412aae841049d8d9b, 'Trader Joe', 'V2 Router'),
     ('arbitrum', 0xb45a2dda996c32e93b8c47098e90ed0e7ab18e39, 'TransitSwap', 'TransitSwapRouterV4'),
     ('arbitrum', 0xaaa87963efeb6f7e0a2711f397663105acb1805e, 'Ramses', 'Router'),
     ('arbitrum', 0xee9dec2712cce65174b561151701bf54b99c24c8, 'Connext', 'ConnextDiamond'),
     ('arbitrum', 0x6131b5fae19ea4f9d964eac0408e4408b66337b5, 'Kyber', 'MetaAggregationRouterV2'),
     ('arbitrum', 0xdef171fe48cf0115b1d80b88dc8eab59176fee57, 'Paraswap', 'AugustusSwapper'),
     ('arbitrum', 0x6947a425453d04305520e612f0cb2952e4d07d62, 'Arbswap', 'ArbswapSmartRouter'),
     ('arbitrum', 0x1342a24347532de79372283b3a29c63c31dd7711, 'Swaprum', 'SwaprumV2Router02'),
     ('arbitrum', 0xf26515d5482e2c2fd237149bf6a653da4794b3d0, 'Solidlizard', 'router'),
     ('arbitrum', 0x0cae51e1032e8461f4806e26332c030e34de3adb, 'AnySwap', 'AnyswapV3Router'),
     ('arbitrum', 0x11111112542d85b3ef69ae05771c2dccff4faa26, '1inch', 'AggregationRouterV3'),
     ('arbitrum', 0x1111111254fb6c44bac0bed2854e76f90643097d, '1inch', 'AggregationRouterV4'),
     ('arbitrum', 0x1111111254eeb25477b68fb85ed929f73a960582, '1inch', 'AggregationRouterV5'),
     ('arbitrum', 0x86d4ef07492605d30124e25b1e08e3c489d39807, 'Lighter', 'Router'),
     ('arbitrum', 0xca10e8825fa9f1db0651cd48a9097997dbf7615d, 'WooFi', 'CrosswapRouterV3.1'),
     ('arbitrum', 0xbbee07b3e8121227afcfe1e2b82772246226128e, 'Vertex', 'TransparentUpgradableProxy'),
     ('arbitrum', 0xc4B2F992496376C6127e73F1211450322E580668, 'Wombat', 'Router'),
     ('arbitrum', 0xba12222222228d8ba445958a75a0704d566bf2c8, 'Balancer', 'Vault'),
     ('arbitrum', 0x8cfe327cec66d1c090dd72bd0ff11d690c33a2eb, 'PancakeSwap', 'Router v2'),
     ('arbitrum', 0xe708aa9e887980750c040a6a2cb901c37aa34f3b, 'Chronos', 'Router'),
     ('arbitrum', 0x16e71B13fE6079B4312063F7E81F76d165Ad32Ad, 'Zyberswap', 'Router')
            
    ) AS x (blockchain, address, dex_name, distinct_name)