{{config(alias='cex_bnb',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    -- Binance
    ('bnb', '0x631Fc1EA2270e98fbD9D92658eCe0F5a269Aa161', 'Binance 1', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0xB1256D6b31E4Ae87DA1D56E5890C66be7f1C038e', 'Binance 2', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x17B692ae403a8Ff3a3B2eD7676cF194310ddE9Af', 'Binance 3', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x8ff804cc2143451f454779a40de386f913dcff20', 'Binance 4', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0xAD9ffffd4573b642959D3B854027735579555Cbc', 'Binance 5', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x8894e0a0c962cb723c1976a4421c95949be2d4e3', 'Binance 6', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0xe2fc31f816a9b94326492132018c3aecc4a93ae1', 'Binance 7', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x3c783c21a0383057d128bae431894a5c19f9cf06', 'Binance 8', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0xdccf3b77da55107280bd850ea519df3705d1a75a', 'Binance 9', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x01c952174c24e1210d26961d456a77a39e1f0bb0', 'Binance 10', 'institution', 'soispoke', 'static', timestamp('2022-22-14'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x161ba15a5f335c9f06bb5bbb0a9ce14076fbb645', 'Binance 11', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x515b72ed8a97f42c568d6a143232775018f133c8', 'Binance 12', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0xbd612a3f30dca67bf60a39fd0d35e39b7ab80774', 'Binance 13', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x7a8A34DB9acD10C3b6277473b192FE47192569cA', 'Binance 14', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x1D40B233CdF2cC0CDC347d5401D5b02c2831A0c1', 'Binance 15', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0xa180fe01b906a1be37be6c534a3300785b20d947', 'Binance 16', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x29bdfbf7d27462a2d115748ace2bd71a2646946c', 'Binance 17', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x73f5ebe90f27b46ea12e5795d16c4b408b19cc6f', 'Binance 18', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x1fbe2acee135d991592f167ac371f3dd893a508b', 'Binance 19', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0xf977814e90da44bfa03b6295a0616a897441acec', 'Binance 20', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8', 'Binance 21', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x5a52e96bacdabb82fd05763e25335261b270efcb', 'Binance 22', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0xeb2d2f1b8c558a40207669291fda468e50c8a0bb', 'Binance Charity', 'institution', 'soispoke', 'static', timestamp('2022-08-28'), now(), 'cex_bnb', 'identifier'),
    -- Crypto.com
    ('bnb', '0x6262998ced04146fa42253a5c0af90ca02dfd2a3', 'Crypto.com 1', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x46340b20830761efd32832a74d7169b29feb9758', 'Crypto.com 2', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x72A53cDBBcc1b9efa39c834A540550e23463AAcB', 'Crypto.com 3', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0x7758e507850da48cd47df1fb5f875c23e3340c50', 'Crypto.com 4', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_bnb', 'identifier'),
    ('bnb', '0xcffad3200574698b78f32232aa9d63eabd290703', 'Crypto.com 5', 'institution', 'soispoke', 'static', timestamp('2022-11-14'), now(), 'cex_bnb', 'identifier')
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)
