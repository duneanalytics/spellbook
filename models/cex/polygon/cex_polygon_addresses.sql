{{config(
        tags = ['static', 'dunesql'],
        alias = alias('addresses'),
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    -- Source: https://snowtrace.io/accounts/label/exchange
    ('polygon', 0x082489a616ab4d46d1947ee3f912e080815b08da, 'Binance', 'Binance 1', 'hildobby', date '2023-04-06')
    , ('polygon', 0xf977814e90da44bfa03b6295a0616a897441acec, 'Binance', 'Binance 2', 'hildobby', date '2023-04-06')
    , ('polygon', 0xe7804c37c13166ff0b37f5ae0bb07a3aebb6e245, 'Binance', 'Binance 3', 'hildobby', date '2023-04-06')
    , ('polygon', 0x51e3d44172868acc60d68ca99591ce4230bc75e0, 'MEXC', 'MEXC 1', 'hildobby', date '2023-04-06')
    , ('polygon', 0x576b81f0c21edbc920ad63feeeb2b0736b018a58, 'MEXC', 'MEXC 2', 'hildobby', date '2023-04-06')
    -- Bitfinex, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitfinex_address.txt
    , ('polygon', 0x876eabf441b2ee5b5b0554fd502a8e0600950cfa, 'Bitfinex', 'Bitfinex 1', 'hildobby', date '2023-04-06')
    , ('polygon', 0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec, 'Bitfinex', 'Bitfinex 2', 'hildobby', date '2023-04-06')
    , ('polygon', 0x742d35Cc6634C0532925a3b844Bc454e4438f44e, 'Bitfinex', 'Bitfinex 3', 'hildobby', date '2023-04-06')
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('polygon', 0x0639556f03714a74a5feeaf5736a4a64ff70d206, 'Bitget', 'Bitget 1', 'hildobby', date '2023-04-06')
    , ('polygon', 0x97b9d2102a9a65a26e1ee82d59e42d1b73b68689, 'Bitget', 'Bitget 2', 'hildobby', date '2023-04-06')
    , ('polygon', 0x5bdf85216ec1e38d6458c870992a69e38e03f7ef, 'Bitget', 'Bitget 3', 'hildobby', date '2023-04-06')
    -- Crypto.com, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/crypto-com/index.js
    , ('polygon', 0xcffad3200574698b78f32232aa9d63eabd290703, 'Crypto.com', 'Crypto.com 1', 'hildobby', date '2023-04-06')
    , ('polygon', 0x6262998ced04146fa42253a5c0af90ca02dfd2a3, 'Crypto.com', 'Crypto.com 2', 'hildobby', date '2023-04-06')
    , ('polygon', 0x72a53cdbbcc1b9efa39c834a540550e23463aacb, 'Crypto.com', 'Crypto.com 3', 'hildobby', date '2023-04-06')
    , ('polygon', 0x7758e507850da48cd47df1fb5f875c23e3340c50, 'Crypto.com', 'Crypto.com 4', 'hildobby', date '2023-04-06')
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('polygon', 0x0d0707963952f2fba59dd06f2b425ace40b492fe, 'Gate.io', 'Gate.io 1', 'hildobby', date '2022-11-14')
    , ('polygon', 0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c, 'Gate.io', 'Gate.io 2', 'hildobby', date '2022-11-14')
    , ('polygon', 0xd793281182a0e3e023116004778f45c29fc14f19, 'Gate.io', 'Gate.io 3', 'hildobby', date '2022-11-14')
    , ('polygon', 0xc882b111a75c0c657fc507c04fbfcd2cc984f071, 'Gate.io', 'Gate.io 4', 'hildobby', date '2022-11-14')
    -- Huobi, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/huobi_address.txt
    , ('polygon', 0xf89d7b9c864f589bbf53a82105107622b35eaa40, 'Huobi', 'Huobi 1', 'hildobby', date '2023-04-06')
    -- Bybit, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/bybit/index.js
    , ('polygon', 0xd70250731a72c33bfb93016e3d1f0ca160df7e42, 'Bybit', 'Bybit 1', 'hildobby', date '2023-04-06')
    -- OKX, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/okx_address.txt
    , ('polygon', 0x06959153b974d0d5fdfd87d561db6d8d4fa0bb0b, 'OKX', 'OKX 1', 'hildobby', date '2023-04-06')
    -- WOO Network, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/woo-cex/index.js
    , ('polygon', 0x0d83f81bc9f1e8252f87a4109bbf0d90171c81df, 'WOO Network', 'WOO Network 1', 'hildobby', date '2023-04-07')
    , ('polygon', 0x63DFE4e34A3bFC00eB0220786238a7C6cEF8Ffc4, 'WOO Network', 'WOO Network 2', 'hildobby', date '2023-04-07')
    , ('polygon', 0xE505Bf08C03cc0FA4e0FDFa2487E2c11085b3FD9, 'WOO Network', 'WOO Network 3', 'hildobby', date '2023-04-07')
    , ('polygon', 0xea319fd75766f5180018f8e760f51c3d3c457496, 'WOO Network', 'WOO Network 4', 'hildobby', date '2023-04-07')
    -- Hotbit, Source: https://etherscan.io/accounts/label/hotbit
    , ('polygon', 0xb34ed85bc0b9da2fa3c5e5d2f4b24f8ee96ce4e9, 'Hotbit', 'Hotbit 1', 'hildobby', date '2022-08-28')
    , ('polygon', 0x768f2a7ccdfde9ebdfd5cea8b635dd590cb3a3f1, 'Hotbit', 'Hotbit 2', 'hildobby', date '2022-08-28')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)