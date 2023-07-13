{{config(
        alias = alias('addresses', legacy_model=True),
        tags=['static'],
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "cex",
                                    \'["soispoke", "hildobby"]\') }}')}}

SELECT blockchain, LOWER(address) AS address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    -- Binance
    ('bnb', '0x631Fc1EA2270e98fbD9D92658eCe0F5a269Aa161', 'Binance', 'Binance 1', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0xB1256D6b31E4Ae87DA1D56E5890C66be7f1C038e', 'Binance', 'Binance 2', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x17B692ae403a8Ff3a3B2eD7676cF194310ddE9Af', 'Binance', 'Binance 3', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x8ff804cc2143451f454779a40de386f913dcff20', 'Binance', 'Binance 4', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0xAD9ffffd4573b642959D3B854027735579555Cbc', 'Binance', 'Binance 5', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x8894e0a0c962cb723c1976a4421c95949be2d4e3', 'Binance', 'Binance 6', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0xe2fc31f816a9b94326492132018c3aecc4a93ae1', 'Binance', 'Binance 7', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x3c783c21a0383057d128bae431894a5c19f9cf06', 'Binance', 'Binance 8', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0xdccf3b77da55107280bd850ea519df3705d1a75a', 'Binance', 'Binance 9', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x01c952174c24e1210d26961d456a77a39e1f0bb0', 'Binance', 'Binance 10', 'soispoke', timestamp '2022-12-14')
    , ('bnb', '0x161ba15a5f335c9f06bb5bbb0a9ce14076fbb645', 'Binance', 'Binance 11', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x515b72ed8a97f42c568d6a143232775018f133c8', 'Binance', 'Binance 12', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0xbd612a3f30dca67bf60a39fd0d35e39b7ab80774', 'Binance', 'Binance 13', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x7a8A34DB9acD10C3b6277473b192FE47192569cA', 'Binance', 'Binance 14', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x1D40B233CdF2cC0CDC347d5401D5b02c2831A0c1', 'Binance', 'Binance 15', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0xa180fe01b906a1be37be6c534a3300785b20d947', 'Binance', 'Binance 16', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x29bdfbf7d27462a2d115748ace2bd71a2646946c', 'Binance', 'Binance 17', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x73f5ebe90f27b46ea12e5795d16c4b408b19cc6f', 'Binance', 'Binance 18', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x1fbe2acee135d991592f167ac371f3dd893a508b', 'Binance', 'Binance 19', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0xf977814e90da44bfa03b6295a0616a897441acec', 'Binance', 'Binance 20', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8', 'Binance', 'Binance 21', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0x5a52e96bacdabb82fd05763e25335261b270efcb', 'Binance', 'Binance 22', 'soispoke', timestamp '2022-08-28')
    , ('bnb', '0xeb2d2f1b8c558a40207669291fda468e50c8a0bb', 'Binance', 'Binance Charity', 'soispoke', timestamp '2022-08-28')
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('bnb', '0x0639556f03714a74a5feeaf5736a4a64ff70d206', 'Bitget', 'Bitget 1', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0x149ded7438caf5e5bfdc507a6c25436214d445e1', 'Bitget', 'Bitget 2', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0x3a7d1a8c3a8dc9d48a68e628432198a2ead4917c', 'Bitget', 'Bitget 3', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0x97b9d2102a9a65a26e1ee82d59e42d1b73b68689', 'Bitget', 'Bitget 4', 'hildobby', timestamp '2023-04-06')
    -- Bybit, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bybit_address.txt
    , ('bnb', '0xee5b5b923ffce93a870b3104b7ca09c3db80047a', 'Bybit', 'Bybit 1', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0xf89d7b9c864f589bbf53a82105107622b35eaa40', 'Bybit', 'Bybit 2', 'hildobby', timestamp '2023-04-06')
    -- Crypto.com
    , ('bnb', '0x6262998ced04146fa42253a5c0af90ca02dfd2a3', 'Crypto.com', 'Crypto.com 1', 'soispoke', timestamp '2022-11-14')
    , ('bnb', '0x46340b20830761efd32832a74d7169b29feb9758', 'Crypto.com', 'Crypto.com 2', 'soispoke', timestamp '2022-11-14')
    , ('bnb', '0x72a53cdbbcc1b9efa39c834a540550e23463aacb', 'Crypto.com', 'Crypto.com 3', 'soispoke', timestamp '2022-11-14')
    , ('bnb', '0x7758e507850da48cd47df1fb5f875c23e3340c50', 'Crypto.com', 'Crypto.com 4', 'soispoke', timestamp '2022-11-14')
    , ('bnb', '0xcffad3200574698b78f32232aa9d63eabd290703', 'Crypto.com', 'Crypto.com 5', 'soispoke', timestamp '2022-11-14')
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('bnb', '0x0d0707963952f2fba59dd06f2b425ace40b492fe', 'Gate.io', 'Gate.io 1', 'hildobby', timestamp '2022-11-14')
    , ('bnb', '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 'Gate.io', 'Gate.io 2', 'hildobby', timestamp '2022-11-14')
    , ('bnb', '0xd793281182a0e3e023116004778f45c29fc14f19', 'Gate.io', 'Gate.io 3', 'hildobby', timestamp '2022-11-14')
    , ('bnb', '0xc882b111a75c0c657fc507c04fbfcd2cc984f071', 'Gate.io', 'Gate.io 4', 'hildobby', timestamp '2022-11-14')
    -- Swissborg, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/swissborg/index.js
    , ('bnb', '0x5770815b0c2a09a43c9e5aecb7e2f3886075b605', 'Swissborg', 'Swissborg 1', 'hildobby', timestamp '2023-04-06')
    -- MaskEX, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/maskex/index.js
    , ('bnb', '0x1349907c197731c5ed98d8442309a15107cb6bad', 'MaskEX', 'MaskEX 1', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0x2161217d22fac0188775432f8ba32f1d4272dd19', 'MaskEX', 'MaskEX 2', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0x3dd878a95dcaef2800cd57bb065b5e8f2f438131', 'MaskEX', 'MaskEX 3', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0x46c75fc52e0263946f8f1a75a95c23a767d2f26e', 'MaskEX', 'MaskEX 4', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0x6e2673095545280f6f10e22eb861a555c6e94bec', 'MaskEX', 'MaskEX 5', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0x84457412efe8b3a05583cb496e1d2c03e6f36155', 'MaskEX', 'MaskEX 6', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0x8458c828d602230e92eb0aac5a6aed5580011b6a', 'MaskEX', 'MaskEX 7', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0xA310b3eecA53B9C115af529faF92Bb5ca4B41494', 'MaskEX', 'MaskEX 8', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0xa4E71851A8c8eaeFeb20A994159F4A443E46059b', 'MaskEX', 'MaskEX 9', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0xc6acb77befebff0359cc581973859eee8cbaeda1', 'MaskEX', 'MaskEX 10', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0xd666ad8d95903bce9b4dcd2cacde5145e36405c2', 'MaskEX', 'MaskEX 11', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0xd7aed730a7c4cf8dfe313b16712af3406f6dca5b', 'MaskEX', 'MaskEX 12', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0x6db133e840376555a5ad5c1d7616872ef57e7f13', 'MaskEX', 'MaskEX 13', 'hildobby', timestamp '2023-04-06')
    , ('bnb', '0xDCa6951B82e82AF6AAB4bB9e90CA00F5760370e1', 'MaskEX', 'MaskEX 14', 'hildobby', timestamp '2023-04-06')
    -- WOO Network, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/woo-cex/index.js
    , ('bnb', '0x0d83f81bc9f1e8252f87a4109bbf0d90171c81df', 'WOO Network', 'WOO Network 1', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0x63DFE4e34A3bFC00eB0220786238a7C6cEF8Ffc4', 'WOO Network', 'WOO Network 2', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0xE505Bf08C03cc0FA4e0FDFa2487E2c11085b3FD9', 'WOO Network', 'WOO Network 3', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0xea319fd75766f5180018f8e760f51c3d3c457496', 'WOO Network', 'WOO Network 4', 'hildobby', timestamp '2023-04-07')
    -- CoinDCX, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/coindcx/index.js
    , ('bnb', '0x4D24EecEcb86041F47bca41265319e9f06aE2Fcb', 'CoinDCX', 'CoinDCX 1', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0x660e3Bd3bcDa11538fa331282666F1d001b87A42', 'CoinDCX', 'CoinDCX 2', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0x8c7Efd5B04331EFC618e8006f19019A3Dc88973e', 'CoinDCX', 'CoinDCX 3', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0xF25d1D2507ce1f956F5BAb45aD2341e3c0DB6d3C', 'CoinDCX', 'CoinDCX 4', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0xF379FcD9C996d85de025985bA9B1C9C96DAa4a72', 'CoinDCX', 'CoinDCX 5', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0xb79421720b92180487f71F13c5D5D8B9ecA27BF1', 'CoinDCX', 'CoinDCX 6', 'hildobby', timestamp '2023-04-07')
    -- Hotbit, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/coindcx/index.js
    , ('bnb', '0xC7029E939075F48fa2D5953381660c7d01570171', 'Hotbit', 'Hotbit 1', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0xb18fbfe3d34fdc227eb4508cde437412b6233121', 'Hotbit', 'Hotbit 2', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0x768f2a7ccdfde9ebdfd5cea8b635dd590cb3a3f1', 'Hotbit', 'Hotbit 3', 'hildobby', timestamp '2023-04-07')
    -- BitVenus, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/bitvenus/index.js
    , ('bnb', '0xef7a2610a7c9cfb2537d68916b6a87fea8acfec3', 'BitVenus', 'BitVenus 1', 'hildobby', timestamp '2023-04-07')
    , ('bnb', '0x4785e47aE7061632C2782384DA28B9F68a5647a3', 'BitVenus', 'BitVenus 2', 'hildobby', timestamp '2023-04-07')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
