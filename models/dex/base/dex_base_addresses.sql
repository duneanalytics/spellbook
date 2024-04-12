{{ config(
    schema = 'dex_base',
    alias = 'addresses',
    tags = ['static']
) }}

SELECT blockchain, address, dex_name, distinct_name
FROM (VALUES
      ('base', 0x22f9dcf4647084d6c31b2765f6910cd85c178c18, '0x', 'Exchange Proxy Flash Wallet'),
      ('base', 0x2626664c2603336e57b271c5c0b26f421741e481, 'Uniswap', 'SwapRouter02'),
      ('base', 0x198ef79f1f515f02dfe9e3115ed9fc07183f02fc, 'Uniswap', 'UniversalRouter'),
      ('base', 0xec8b0f7ffe3ae75d7ffab09429e3675bb63503e4, 'Uniswap', 'UniversalRouter'),
      ('base', 0xdef1c0ded9bec7f1a1670819833240f027b25eff, 'ZeroEx', 'ExchangeProxy'),
      ('base', 0x1b8eea9315be495187d873da7773a874545d9d48, 'BaseSwap', 'SwapRouter'),
      ('base', 0xcf77a3ba9a5ca399b7c97c74d54e5b1beb874e43, 'Aerodrome', 'SwapRouter'),
      ('base', 0x708845b2a00dea5d7b0dacf4a84faa51d358e4b2, 'Aerodrome', 'SmartRouter'),
      ('base', 0xf9cfb8a62f50e10adde5aa888b44cf01c5957055, 'Aerodrome', 'SmartRouter'),
      ('base', 0xb556ee2761f5d2887b8f35a7dda367abd20503bf, 'Aerodrome', 'AeroVault'),
      ('base', 0x32aed3bce901da12ca8489788f3a99fce1056e14, 'Maverick', 'Router'),
      ('base', 0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86, 'PancakeSwap', 'SmartRouter'),
      ('base', 0xb0e66ff71869815f2c2e14af9e039882cf0795ef, 'PancakeSwap', 'SmartRouter'),
      ('base', 0x1111111254eeb25477b68fb85ed929f73a960582, '1inch', 'AggregationRouterV5'),
      ('base', 0x11111112542d85b3ef69ae05771c2dccff4faa26, '1inch', 'AggregationRouterV4'),
      ('base', 0xf8b959870634e1bb1926f8790e5ec3592d44a82a, 'DackieSwap', 'Router'),
      ('base', 0x19ceead7105607cd444f5ad10dd51356436095a1, 'Odos', 'Router'),
      ('base', 0x3a23f943181408eac424116af7b7790c94cb97a5, 'Socket', 'Aggregator of Aggregators'),
      ('base', 0x00000000009726632680fb29d3f7a9734e3010e2, 'Rainbow', 'Aggregator of Aggregators')

    ) AS x (blockchain, address, dex_name, distinct_name)