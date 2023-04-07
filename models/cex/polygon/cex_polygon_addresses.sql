{{config(alias='addresses',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, LOWER(address) AS address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    -- Source: https://snowtrace.io/accounts/label/exchange
    ('polygon', '0x082489a616ab4d46d1947ee3f912e080815b08da', 'Binance 1', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0xf977814e90da44bfa03b6295a0616a897441acec', 'Binance 2', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0xe7804c37c13166ff0b37f5ae0bb07a3aebb6e245', 'Binance 3', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0x51e3d44172868acc60d68ca99591ce4230bc75e0', 'MEXC 1', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0x576b81f0c21edbc920ad63feeeb2b0736b018a58', 'MEXC 2', 'hildobby', timestamp('2023-04-06'))
    -- Bitfinex, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitfinex_address.txt
    , ('polygon', '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa', 'Bitfinex 1', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec', 'Bitfinex 2', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0x742d35Cc6634C0532925a3b844Bc454e4438f44e', 'Bitfinex 3', 'hildobby', timestamp('2023-04-06'))
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('polygon', '0x0639556f03714a74a5feeaf5736a4a64ff70d206', 'Bitget 1', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0x97b9d2102a9a65a26e1ee82d59e42d1b73b68689', 'Bitget 2', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0x5bdf85216ec1e38d6458c870992a69e38e03f7ef', 'Bitget 3', 'hildobby', timestamp('2023-04-06'))
    -- Crypto.com, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/crypto-com/index.js
    , ('polygon', '0xcffad3200574698b78f32232aa9d63eabd290703', 'Crypto.com 1', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0x6262998ced04146fa42253a5c0af90ca02dfd2a3', 'Crypto.com 2', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0x72a53cdbbcc1b9efa39c834a540550e23463aacb', 'Crypto.com 3', 'hildobby', timestamp('2023-04-06'))
    , ('polygon', '0x7758e507850da48cd47df1fb5f875c23e3340c50', 'Crypto.com 4', 'hildobby', timestamp('2023-04-06'))
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('polygon', '0x0d0707963952f2fba59dd06f2b425ace40b492fe', 'Gate.io 1', 'hildobby', timestamp('2022-11-14'))
    , ('polygon', '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 'Gate.io 2', 'hildobby', timestamp('2022-11-14'))
    , ('polygon', '0xd793281182a0e3e023116004778f45c29fc14f19', 'Gate.io 3', 'hildobby', timestamp('2022-11-14'))
    , ('polygon', '0xc882b111a75c0c657fc507c04fbfcd2cc984f071', 'Gate.io 4', 'hildobby', timestamp('2022-11-14'))
    -- Huobi, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/huobi_address.txt
    , ('polygon', '0xf89d7b9c864f589bbf53a82105107622b35eaa40', 'Huobi', 'hildobby', timestamp('2023-04-06'))
    -- Bybit, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/bybit/index.js
    , ('polygon', '0xd70250731a72c33bfb93016e3d1f0ca160df7e42', 'Huobi', 'hildobby', timestamp('2023-04-06'))
    -- OKX, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/okx_address.txt
    , ('polygon', '0x06959153b974d0d5fdfd87d561db6d8d4fa0bb0b', 'OKX', 'hildobby', timestamp('2023-04-06'))
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)