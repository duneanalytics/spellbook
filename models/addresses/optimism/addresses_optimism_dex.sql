{{config(alias = 'dex',
        tags=['static']

SELECT address, dex_name, distinct_name
FROM (VALUES
       ('0x11111112542d85b3ef69ae05771c2dccff4faa26', '1inch', 'AggregationRouterV3'),
      ('0x1111111254760f7ab3f16433eea9304126dcd199', '1inch', 'AggregationRouterV4'),
      ('0x1111111254eeb25477b68fb85ed929f73a960582', '1inch', 'AggregationRouterV5'),
      ('0x00c0184c0b5d42fba6b7ca914b31239b419ab80b', 'Slingshot Finance', 'Swap'),
      ('0x8b396ddf906d552b2f98a8e7d743dd58cd0d920f', 'SushiSwap', 'SushiXSwap'),
      ('0x4c5d5234f232bd2d76b96aa33f5ae4fcf0e4bfab', 'SushiSwap', 'RouteProcessor3'),
      ('0xe592427a0aece92de3edee1f18e0157c05861564', 'Uniswap', 'SwapRouter'),
      ('0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45', 'Uniswap', 'SwapRouter02'),
      ('0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad', 'Uniswap', 'UniversalRouter'),
      ('0xdef1abe32c034e558cdd535791643c58a13acc10', 'ZeroEx', 'ExchangeProxy'),
      ('0x5130f6ce257b8f9bf7fac0a0b519bd588120ed40', 'Clipper', 'ClipperPackedVerifiedExchange'),
      ('0xa132dab612db5cb9fc9ac426a0cc215a3423f9c9', 'Velodrome', 'Router'),
      ('0x9c12939390052919af3155f41bf4160fd3666a6f', 'Velodrome', 'Router'),
      ('0xc30141b657f4216252dc59af2e7cdb9d8792e1b0', 'Socket', 'Registry'),
      ('0x69dd38645f7457be13571a847ffd905f9acbaf6d', 'Odos', 'OdosRouter'),
      ('0xeaf1ac8e89ea0ae13e0f03634a4ff23502527024', 'WooFi', 'WooRouter'),
      ('0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae', 'LiFi', 'LiFiDiamond_v2'),
      ('0x777777773fdd8b28bb03377d10fcea75ad9768da', 'Via router', 'ViaRouter'),
      ('0x82ac2ce43e33683c58be4cdc40975e73aa50f459', 'Perp', 'ClearingHouse'),
      ('0xdef171fe48cf0115b1d80b88dc8eab59176fee57', 'Paraswap', 'AugustusSwapper'),
      ('0x0c6134abc08a1eafc3e2dc9a5ad023bb08da86c3', 'Firebird', 'FireBirdRouter'),
      ('0x6352a56caadc4f1e25cd6c75970fa768a3304e64', 'OpenOcean', 'OpenOceanExchangeProxy')
      
    ) AS x (address, dex_name, distinct_name)