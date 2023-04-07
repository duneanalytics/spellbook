{{config(alias='addresses',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "cex",
                                    \'["soispoke", "hildobby"]\') }}')}}

SELECT blockchain, LOWER(address) AS address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    -- Binance
    ('bnb', '0x631Fc1EA2270e98fbD9D92658eCe0F5a269Aa161', 'Binance 1', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0xB1256D6b31E4Ae87DA1D56E5890C66be7f1C038e', 'Binance 2', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x17B692ae403a8Ff3a3B2eD7676cF194310ddE9Af', 'Binance 3', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x8ff804cc2143451f454779a40de386f913dcff20', 'Binance 4', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0xAD9ffffd4573b642959D3B854027735579555Cbc', 'Binance 5', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x8894e0a0c962cb723c1976a4421c95949be2d4e3', 'Binance 6', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0xe2fc31f816a9b94326492132018c3aecc4a93ae1', 'Binance 7', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x3c783c21a0383057d128bae431894a5c19f9cf06', 'Binance 8', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0xdccf3b77da55107280bd850ea519df3705d1a75a', 'Binance 9', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x01c952174c24e1210d26961d456a77a39e1f0bb0', 'Binance 10', 'soispoke', timestamp('2022-22-14'))
    , ('bnb', '0x161ba15a5f335c9f06bb5bbb0a9ce14076fbb645', 'Binance 11', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x515b72ed8a97f42c568d6a143232775018f133c8', 'Binance 12', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0xbd612a3f30dca67bf60a39fd0d35e39b7ab80774', 'Binance 13', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x7a8A34DB9acD10C3b6277473b192FE47192569cA', 'Binance 14', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x1D40B233CdF2cC0CDC347d5401D5b02c2831A0c1', 'Binance 15', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0xa180fe01b906a1be37be6c534a3300785b20d947', 'Binance 16', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x29bdfbf7d27462a2d115748ace2bd71a2646946c', 'Binance 17', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x73f5ebe90f27b46ea12e5795d16c4b408b19cc6f', 'Binance 18', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x1fbe2acee135d991592f167ac371f3dd893a508b', 'Binance 19', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0xf977814e90da44bfa03b6295a0616a897441acec', 'Binance 20', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8', 'Binance 21', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0x5a52e96bacdabb82fd05763e25335261b270efcb', 'Binance 22', 'soispoke', timestamp('2022-08-28'))
    , ('bnb', '0xeb2d2f1b8c558a40207669291fda468e50c8a0bb', 'Binance Charity', 'soispoke', timestamp('2022-08-28')),
    -- Bitget, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bitget_address.txt
    , ('bnb', '0x0639556f03714a74a5feeaf5736a4a64ff70d206', 'Bitget 1', 'hildobby', timestamp('2023-04-06'))
    , ('bnb', '0x149ded7438caf5e5bfdc507a6c25436214d445e1', 'Bitget 2', 'hildobby', timestamp('2023-04-06'))
    , ('bnb', '0x3a7d1a8c3a8dc9d48a68e628432198a2ead4917c', 'Bitget 3', 'hildobby', timestamp('2023-04-06'))
    , ('bnb', '0x97b9d2102a9a65a26e1ee82d59e42d1b73b68689', 'Bitget 4', 'hildobby', timestamp('2023-04-06')),
    -- Bybit, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/bybit_address.txt
    , ('bnb', '0xee5b5b923ffce93a870b3104b7ca09c3db80047a', 'Bybit 1', 'hildobby', timestamp('2023-04-06'))
    , ('bnb', '0xf89d7b9c864f589bbf53a82105107622b35eaa40', 'Bybit 2', 'hildobby', timestamp('2023-04-06')),
    -- Crypto.com
    , ('bnb', '0x6262998ced04146fa42253a5c0af90ca02dfd2a3', 'Crypto.com 1', 'soispoke', timestamp('2022-11-14'))
    , ('bnb', '0x46340b20830761efd32832a74d7169b29feb9758', 'Crypto.com 2', 'soispoke', timestamp('2022-11-14'))
    , ('bnb', '0x72a53cdbbcc1b9efa39c834a540550e23463aacb', 'Crypto.com 3', 'soispoke', timestamp('2022-11-14'))
    , ('bnb', '0x7758e507850da48cd47df1fb5f875c23e3340c50', 'Crypto.com 4', 'soispoke', timestamp('2022-11-14'))
    , ('bnb', '0xcffad3200574698b78f32232aa9d63eabd290703', 'Crypto.com 5', 'soispoke', timestamp('2022-11-14'))
    -- Gate.io, source: https://raw.githubusercontent.com/js-kingdata/indicators_factory/fefe53bca88ecf331a71fc59e34aab319f3415c5/crawlers/address_tags/cex/gate-io_address.txt
    , ('bnb', '0x0d0707963952f2fba59dd06f2b425ace40b492fe', 'Gate.io 1', 'hildobby', timestamp('2022-11-14'))
    , ('bnb', '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c', 'Gate.io 2', 'hildobby', timestamp('2022-11-14'))
    , ('bnb', '0xd793281182a0e3e023116004778f45c29fc14f19', 'Gate.io 3', 'hildobby', timestamp('2022-11-14'))
    , ('bnb', '0xc882b111a75c0c657fc507c04fbfcd2cc984f071', 'Gate.io 4', 'hildobby', timestamp('2022-11-14'))
    -- Swissborg, source: https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/swissborg/index.js
    , ('bnb', '0x5770815b0c2a09a43c9e5aecb7e2f3886075b605', 'Swissborg 1', 'hildobby', timestamp('2023-04-06'))
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
