{{config(
        tags = ['static'],
        schema = 'cex_aptos',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["aptos"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

-- from https://github.com/aptos-labs/explorer/blob/main/src/constants.tsx
SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('aptos', '0x5bd7de5c56d5691f32ea86c973c73fec7b1445e59736c97158020018c080bb00', 'Binance', 'Binance 1', 'hildobby', date '2024-04-20')
    , ('aptos', '0x80174e0fe8cb2d32b038c6c888dd95c3e1560736f0d4a6e8bed6ae43b5c91f6f', 'Binance', 'Binance 2', 'hildobby', date '2024-04-20')
    , ('aptos', '0xae1a6f3d3daccaf77b55044cea133379934bba04a11b9d0bbd643eae5e6e9c70', 'Binance', 'Binance 3', 'hildobby', date '2024-04-20')
    , ('aptos', '0xd91c64b777e51395c6ea9dec562ed79a4afa0cd6dad5a87b187c37198a1f855a', 'Binance', 'Binance 4', 'hildobby', date '2024-04-20')
    , ('aptos', '0xed8c46bec9dbc2b23c60568f822b95b87ea395f7e3fdb5e3adc0a30c55c0a60e', 'Binance', 'Binance 5', 'hildobby', date '2024-04-20')
    , ('aptos', '0xbdb53eb583ba02ab0606bdfc71b59a191400f75fb62f9df124494ab877cdfe2a', 'Binance', 'Binance 6', 'ying-w', date '2024-07-14')
    , ('aptos', '0x33f91e694d40ca0a14cb84e1f27a4d03de5cf292b07ed75ed3286e4f243dab34', 'Binance', 'Binance 7', 'ying-w', date '2024-07-14')
    , ('aptos', '0x1d14ee0c332546658b13965a39faf5ec24ad195b722435d9fe23dc55487e67e3', 'Binance', 'Binance 9', 'ying-w', date '2024-07-14')
    , ('aptos', '0xfd9192f8ad8dc60c483a884f0fbc8940f5b8618f3cf2bbf91693982b373dfdea', 'Bitfinex', 'Bitfinex 1', 'hildobby', date '2024-04-20')
    , ('aptos', '0x834d639b10d20dcb894728aa4b9b572b2ea2d97073b10eacb111f338b20ea5d7', 'OKX', 'OKX 1', 'hildobby', date '2024-04-20')
    , ('aptos', '0x8f347361a9461e9312a4d2b5b5b928c65c3a740965705361317e3ca0015c64d8', 'OKX', 'OKX 2', 'ying-w', date '2024-07-14')
    , ('aptos', '0x966e3ee07a3403a72f44c53f457d34c7148c2c8812c8d52509f54d4a00a36c41', 'OKX', 'OKX 3', 'ying-w', date '2024-07-14')
    , ('aptos', '0x51c6abe562e755582d268340b2cf0e2d8895a155dc9b7a7fb5465000d62d770b', 'OKX', 'OKX 4', 'ying-w', date '2024-07-14')
    , ('aptos', '0x3621fa917ffef3f0509f5d5953672a69791df329139644d89d0a1b0beb98c585', 'OKX', 'OKX 5', 'ying-w', date '2024-07-14')
    , ('aptos', '0x4c1ef44079cb31851349fba50d385f708a10ec7ac612859fdbf28888d1f7b572', 'OKX', 'OKX 6', 'ying-w', date '2024-07-14')
    , ('aptos', '0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0', 'Bybit', 'Bybit 1', 'ying-w', date '2024-07-14')
    , ('aptos', '0xdc7adffa09da5736ce1303f7441f4367fa423617c6822ad2fbc8522d9efd8fa4', 'Kraken', 'Kraken 1', 'ying-w', date '2024-07-14')
    , ('aptos', '0x0cf869189c785beaaad2f5c636ced4805aeae9cbf49070dc93aed2f16b99012a', 'Gate', 'Gate 1', 'ying-w', date '2024-07-14')
    , ('aptos', '0x0b3581f46ac8a6920fc9b87fecb7b459b9b39c177e65233826a7b4978bad41cd', 'Coinbase', 'Coinbase 1', 'ying-w', date '2024-07-14')
    , ('aptos', '0xa4e7455d27731ab857e9701b1e6ed72591132b909fe6e4fd99b66c1d6318d9e8', 'Coinbase', 'Coinbase 2', 'ying-w', date '2024-07-14')
    , ('aptos', '0xe8ca094fec460329aaccc2a644dc73c5e39f1a2ad6e97f82b6cbdc1a5949b9ea', 'MEXC', 'MEXC 1', 'ying-w', date '2024-07-14')
    , ('aptos', '0xde084991b91637a08e4da2f1b398f5f935e1393b65d13cc99c597ec5dc105b6b', 'Crypto.com', 'Crypto.com 1', 'ying-w', date '2024-07-14')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
