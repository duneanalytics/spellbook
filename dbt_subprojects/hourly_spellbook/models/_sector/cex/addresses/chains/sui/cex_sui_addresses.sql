{{config(
        tags = ['static'],
        schema = 'cex_sui',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["sui"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('sui', 0x5fdfcc18e0791862c107c49ea13a5bcf4965f00ac057f56ea04034ebb5ea45ad, 'Binance', 'Binance 1', 'hildobby', date '2025-07-07')
    , ('sui', 0x5be98f6812b102e8677cb9afa5644b35c4e6198be7330121dbda04ac2aedba44, 'Binance', 'Binance 2', 'hildobby', date '2025-07-07')
    , ('sui', 0x7ab9a6a7109dcb9cb357a109f32dfcc78a7aa2d6029084eb924d95133fc71cec, 'Binance', 'Binance 3', 'hildobby', date '2025-07-07')
    , ('sui', 0x8d4e8e88447f95a8509de759bf649a51876581f6a6338a9b3c68f82cfb6edd9b, 'Binance', 'Binance 4', 'hildobby', date '2025-07-07')
    , ('sui', 0x935029ca5219502a47ac9b69f556ccf6e2198b5e7815cf50f68846f723739cbd, 'Binance', 'Binance 5', 'hildobby', date '2025-07-07')
    , ('sui', 0x96ed379243830efc1adfe2c1359670f8289561c7f0d1b8810b3db28a827fbe51, 'Binance', 'Binance 6', 'hildobby', date '2025-07-07')
    , ('sui', 0xac3034b15f40ea238c0f9f19f87b1692308219081e72d2798c65167693388082, 'Binance', 'Binance 7', 'hildobby', date '2025-07-07')
    , ('sui', 0xac5bceec1b789ff840d7d4e6ce4ce61c90d190a7f8c4f4ddf0bff6ee2413c33c, 'Binance', 'Binance 8', 'hildobby', date '2025-07-07')

    , ('sui', 0x902b2e182a16fcf17ee984aa0d6f502ab77619e5187f250e7639497f4f4f5ee1, 'Ceffu', 'Ceffu 1', 'hildobby', date '2025-07-07')
    , ('sui', 0xec8bfd046b1450ae97821a3cf86f6e5d21bb9491a590418f5a6aee682a159081, 'Ceffu', 'Ceffu 2', 'hildobby', date '2025-07-07')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
