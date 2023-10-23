{{config(
        tags = ['static'],
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "cex",
                                    \'["Henrystats", "hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    -- Source https://ftmscan.com/accounts/label/exchange
    ('fantom', 0x8e1701cfd85258ddb8dfe89bc4c7350822b9601d, 'MEXC', 'MEXC: Hot Wallet', 'Henrystats', date '2023-01-27')
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('fantom', 0x5bdf85216ec1e38d6458c870992a69e38e03f7ef, 'Bitget', 'Bitget 1', 'hildobby', date '2023-04-06')
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('fantom', 0x0d0707963952f2fba59dd06f2b425ace40b492fe, 'Gate.io', 'Gate.io 1', 'hildobby', date '2023-04-06')
    , ('fantom', 0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c, 'Gate.io', 'Gate.io 2', 'hildobby', date '2023-04-06')
    , ('fantom', 0xd793281182a0e3e023116004778f45c29fc14f19, 'Gate.io', 'Gate.io 3', 'hildobby', date '2023-04-06')
    , ('fantom', 0xc882b111a75c0c657fc507c04fbfcd2cc984f071, 'Gate.io', 'Gate.io 4', 'hildobby', date '2023-04-06')
    -- Crypto.com, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/crypto-com/index.js
    , ('fantom', 0x6262998ced04146fa42253a5c0af90ca02dfd2a3, 'Crypto.com', 'Crypto.com 1', 'hildobby', date '2023-04-06')
    , ('fantom', 0x72a53cdbbcc1b9efa39c834a540550e23463aacb, 'Crypto.com', 'Crypto.com 2', 'hildobby', date '2023-04-06')
    , ('fantom', 0x7758e507850da48cd47df1fb5f875c23e3340c50, 'Crypto.com', 'Crypto.com 3', 'hildobby', date '2023-04-06')
    , ('fantom', 0xcffad3200574698b78f32232aa9d63eabd290703, 'Crypto.com', 'Crypto.com 4', 'hildobby', date '2023-04-06')
    -- WOO Network, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/woo-cex/index.js
    , ('fantom', 0x0d83f81bc9f1e8252f87a4109bbf0d90171c81df, 'WOO Network', 'WOO Network 1', 'hildobby', date '2023-04-07')
    , ('fantom', 0x63DFE4e34A3bFC00eB0220786238a7C6cEF8Ffc4, 'WOO Network', 'WOO Network 2', 'hildobby', date '2023-04-07')
    , ('fantom', 0xE505Bf08C03cc0FA4e0FDFa2487E2c11085b3FD9, 'WOO Network', 'WOO Network 3', 'hildobby', date '2023-04-07')
    , ('fantom', 0xea319fd75766f5180018f8e760f51c3d3c457496, 'WOO Network', 'WOO Network 4', 'hildobby', date '2023-04-07')
    -- Hotbit, Source: https://etherscan.io/accounts/label/hotbit
    , ('fantom', 0xc62A0781934744E05927ceABB94a3043CdCfEA89, 'Hotbit', 'Hotbit 1', 'hildobby', date '2022-08-28')
    , ('fantom', 0x768f2a7ccdfde9ebdfd5cea8b635dd590cb3a3f1, 'Hotbit', 'Hotbit 2', 'hildobby', date '2022-08-28')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)