{{config(alias = alias('addresses', legacy_model=True),
        tags=['legacy', 'static'],
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, LOWER(address) AS address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    -- Source: https://snowtrace.io/accounts/label/exchange
    ('avalanche_c', '0x14aa1ad09664c33679ae5689d93085b8f7c84bd3', 'Coinsquare', 'Coinsquare 1', 'hildobby', timestamp '2023-04-06')
    , ('avalanche_c', '0xffb3118124cdaebd9095fa9a479895042018cac2', 'MEXC', 'MEXC 1', 'hildobby', timestamp '2023-04-06')
    -- Binance, source: https://snowtrace.io/accounts/label/binance
    , ('avalanche_c', '0x9f8c163cba728e99993abe7495f06c0a3c8ac8b9', 'Binance', 'Binance 1', 'hildobby', timestamp '2023-04-06')
    -- Bitfinex, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitfinex_address.txt
    , ('avalanche_c', '0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec', 'Bitfinex', 'Bitfinex 1', 'hildobby', timestamp '2023-04-06')
    , ('avalanche_c', '0x742d35cc6634c0532925a3b844bc454e4438f44e', 'Bitfinex', 'Bitfinex 2', 'hildobby', timestamp '2023-04-06')
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('avalanche_c', '0x0639556f03714a74a5feeaf5736a4a64ff70d206', 'Bitget', 'Bitget 1', 'hildobby', timestamp '2023-04-06')
    , ('avalanche_c', '0x5bdf85216ec1e38d6458c870992a69e38e03f7ef', 'Bitget', 'Bitget 2', 'hildobby', timestamp '2023-04-06')
    -- Bybit, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bybit_address.txt
    , ('avalanche_c', '0xf89d7b9c864f589bbf53a82105107622b35eaa40', 'Bybit', 'Bybit 1', 'hildobby', timestamp '2023-04-06')
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('avalanche_c', '0x0d0707963952f2fba59dd06f2b425ace40b492fe', 'Gate.io', 'Gate.io 1', 'hildobby', timestamp '2022-11-14')
    , ('avalanche_c', '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 'Gate.io', 'Gate.io 2', 'hildobby', timestamp '2022-11-14')
    , ('avalanche_c', '0xd793281182a0e3e023116004778f45c29fc14f19', 'Gate.io', 'Gate.io 3', 'hildobby', timestamp '2022-11-14')
    , ('avalanche_c', '0xc882b111a75c0c657fc507c04fbfcd2cc984f071', 'Gate.io', 'Gate.io 4', 'hildobby', timestamp '2022-11-14')
    -- Huobi, source: http0xf89d7b9c864f589bbF53a82105107622B35EaA40s://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/huobi_address.txt
    , ('avalanche_c', '0xe195b82df6a797551eb1acd506e892531824af27', 'Huobi', 'Huobi 1', 'hildobby', timestamp '2023-04-06')
    -- OKX, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/okx_address.txt
    , ('avalanche_c', '0x7e4aa755550152a522d9578621ea22edab204308', 'OKX', 'OKX 1', 'hildobby', timestamp '2023-04-06')
    -- Crypto.com, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/crypto-com/index.js
    , ('avalanche_c', '0xcffad3200574698b78f32232aa9d63eabd290703', 'Crypto.com', 'Crypto.com 1', 'hildobby', timestamp '2023-04-06')
    , ('avalanche_c', '0x6262998ced04146fa42253a5c0af90ca02dfd2a3', 'Crypto.com', 'Crypto.com 2', 'hildobby', timestamp '2023-04-06')
    , ('avalanche_c', '0x72a53cdbbcc1b9efa39c834a540550e23463aacb', 'Crypto.com', 'Crypto.com 3', 'hildobby', timestamp '2023-04-06')
    , ('avalanche_c', '0x7758e507850da48cd47df1fb5f875c23e3340c50', 'Crypto.com', 'Crypto.com 4', 'hildobby', timestamp '2023-04-06')
    -- WOO Network, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/woo-cex/index.js
    , ('avalanche_c', '0x0d83f81bc9f1e8252f87a4109bbf0d90171c81df', 'WOO Network', 'WOO Network 1', 'hildobby', timestamp '2023-04-07')
    , ('avalanche_c', '0x63DFE4e34A3bFC00eB0220786238a7C6cEF8Ffc4', 'WOO Network', 'WOO Network 2', 'hildobby', timestamp '2023-04-07')
    , ('avalanche_c', '0xE505Bf08C03cc0FA4e0FDFa2487E2c11085b3FD9', 'WOO Network', 'WOO Network 3', 'hildobby', timestamp '2023-04-07')
    , ('avalanche_c', '0xea319fd75766f5180018f8e760f51c3d3c457496', 'WOO Network', 'WOO Network 4', 'hildobby', timestamp '2023-04-07')
    -- Hotbit, Source: https://etherscan.io/accounts/label/hotbit
    , ('avalanche_c', '0x6C2e8d4F73f6A129843d1b3D2ACAFF1DB22E3366', 'Hotbit', 'Hotbit 1', 'hildobby', timestamp '2022-08-28')
    , ('avalanche_c', '0x768f2a7ccdfde9ebdfd5cea8b635dd590cb3a3f1', 'Hotbit', 'Hotbit 2', 'hildobby', timestamp '2022-08-28')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)