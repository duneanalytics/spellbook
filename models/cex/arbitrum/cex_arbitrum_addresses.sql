{{config(alias='addresses',
        tags=['static'],
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, LOWER(address) AS address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    -- Binance, source: https://arbiscan.io/accounts/label/exchange
    ('arbitrum', '0xb38e8c17e38363af6ebdcb3dae12e0243582891d', 'Binance', 'Binance: Hot Wallet', 'hildobby', timestamp '2023-04-06')
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('arbitrum', '0x0639556f03714a74a5feeaf5736a4a64ff70d206', 'Bitget', 'Bitget 1', 'hildobby', timestamp '2023-04-06')
    , ('arbitrum', '0x97b9d2102a9a65a26e1ee82d59e42d1b73b68689', 'Bitget', 'Bitget 2', 'hildobby', timestamp '2023-04-06')
    -- Bybit, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('arbitrum', '0xf89d7b9c864f589bbf53a82105107622b35eaa40', 'Bybit', 'Bybit 1', 'hildobby', timestamp '2023-04-06')
    -- Crypto.com, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/crypto-com/index.js
    , ('arbitrum', '0xcffad3200574698b78f32232aa9d63eabd290703', 'Crypto.com', 'Crypto.com 1', 'hildobby', timestamp '2023-04-06')
    , ('arbitrum', '0x6262998ced04146fa42253a5c0af90ca02dfd2a3', 'Crypto.com', 'Crypto.com 2', 'hildobby', timestamp '2023-04-06')
    , ('arbitrum', '0x72a53cdbbcc1b9efa39c834a540550e23463aacb', 'Crypto.com', 'Crypto.com 3', 'hildobby', timestamp '2023-04-06')
    , ('arbitrum', '0x7758e507850da48cd47df1fb5f875c23e3340c50', 'Crypto.com', 'Crypto.com 4', 'hildobby', timestamp '2023-04-06')
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('arbitrum', '0x0d0707963952f2fba59dd06f2b425ace40b492fe', 'Gate.io', 'Gate.io 1', 'hildobby', timestamp '2022-11-14')
    , ('arbitrum', '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 'Gate.io', 'Gate.io 2', 'hildobby', timestamp '2022-11-14')
    , ('arbitrum', '0xd793281182a0e3e023116004778f45c29fc14f19', 'Gate.io', 'Gate.io 3', 'hildobby', timestamp '2022-11-14')
    , ('arbitrum', '0xc882b111a75c0c657fc507c04fbfcd2cc984f071', 'Gate.io', 'Gate.io 4', 'hildobby', timestamp '2022-11-14')
    -- KuCoin, source: https://github.com/js-kingdata/indicators_factory/blob/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/kucoin_address.txt
    , ('arbitrum', '0xd6216fc19db775df9774a6e33526131da7d19a2c', 'KuCoin', 'KuCoin 1', 'hildobby', timestamp '2022-11-14')
    , ('arbitrum', '0x03e6fa590cadcf15a38e86158e9b3d06ff3399ba', 'KuCoin', 'KuCoin 2', 'hildobby', timestamp '2022-11-14')
    , ('arbitrum', '0xf3f094484ec6901ffc9681bcb808b96bafd0b8a8', 'KuCoin', 'KuCoin 3', 'hildobby', timestamp '2022-11-14')
    -- OKX, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/okx_address.txt
    , ('arbitrum', '0x62383739d68dd0f844103db8dfb05a7eded5bbe6', 'OKX', 'OKX 1', 'hildobby', timestamp '2022-11-14')
    -- WOO Network, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/woo-cex/index.js
    , ('arbitrum', '0x0d83f81bc9f1e8252f87a4109bbf0d90171c81df', 'WOO Network', 'WOO Network 1', 'hildobby', timestamp '2023-04-07')
    , ('arbitrum', '0x63DFE4e34A3bFC00eB0220786238a7C6cEF8Ffc4', 'WOO Network', 'WOO Network 2', 'hildobby', timestamp '2023-04-07')
    , ('arbitrum', '0xE505Bf08C03cc0FA4e0FDFa2487E2c11085b3FD9', 'WOO Network', 'WOO Network 3', 'hildobby', timestamp '2023-04-07')
    , ('arbitrum', '0xea319fd75766f5180018f8e760f51c3d3c457496', 'WOO Network', 'WOO Network 4', 'hildobby', timestamp '2023-04-07')
    -- Hotbit, Source: https://etherscan.io/accounts/label/hotbit
    , ('arbitrum', '0xd690a9DfD7e4B02898Cdd1a9E50eD1fd7D3d3442', 'Hotbit', 'Hotbit 1', 'hildobby', timestamp '2022-08-28')
    , ('arbitrum', '0x768f2a7ccdfde9ebdfd5cea8b635dd590cb3a3f1', 'Hotbit', 'Hotbit 2', 'hildobby', timestamp '2022-08-28')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)