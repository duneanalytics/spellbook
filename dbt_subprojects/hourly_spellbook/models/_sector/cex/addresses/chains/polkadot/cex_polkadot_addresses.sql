{{config(
        tags = ['static'],
        schema = 'cex_polkadot',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["polkadot"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('polkadot', '13VagdYbCRMSBSbmz4UivPpS9SwmTTRiPtMkjoEHRm8vAkqv', 'Binance', 'Binance 1', 'hildobby', date '2024-04-20')
    , ('polkadot', '16ZL8yLyXv3V3L3z9ofR1ovFLziyXaN1DPq4yffMAZ9czzBD', 'Binance', 'Binance 2', 'hildobby', date '2024-04-20')
    , ('polkadot', '1743nDTMZisPgBCYSAgkUn1kVG7MePc9rvMEjoRNf4ipVkF', 'Binance', 'Binance 3', 'hildobby', date '2024-04-20')
    , ('polkadot', '1P6bgxZi42kYYV545c3RSp7NJLUgASDpMP1ifXJazVR1e2N', 'Binance', 'Binance 4', 'hildobby', date '2024-04-20')
    , ('polkadot', '1qnJN7FViy3HZaxZK9tGAA71zxHSBeUweirKqCaox4t8GT7', 'Binance', 'Binance 5', 'hildobby', date '2024-04-20')
    , ('polkadot', '12T1tgaYZzEkFpnPvyqttmPRJxbGbR4uDx49cvZR5SRF8QDu', 'Bitfinex', 'Bitfinex 1', 'hildobby', date '2024-04-20')
    , ('polkadot', '1UJSCYLh44UYhkm1WwXAwT2W8nirTD74VzPsdhfsstY8S3u', 'Bitfinex', 'Bitfinex 2', 'hildobby', date '2024-04-20')
    , ('polkadot', '13AE11jLvxcxsjqaSoWFXCTGUfbjXb1gmZTY8x3TXJzWutmf', 'Bitfinex', 'Bitfinex 3', 'hildobby', date '2024-04-20')
    , ('polkadot', '1347e3PfJKKcJL4XJhFeZ5UmZYRnk26Vs9aGjZ8RZLPkWWNY', 'LAToken', 'LAToken 1', 'hildobby', date '2024-04-20')
    , ('polkadot', '14B3z6xL9vGgKz8WptoZabPrgH6adH1ev2Ven4SiTcdznfqd', 'OKX', 'OKX 1', 'hildobby', date '2024-04-20')
    , ('polkadot', '15abPBmJrMY7QJeCEQJQbQ9a62A7ndfTo8KC7Wn4dzt9zMMg', 'OKX', 'OKX 2', 'hildobby', date '2024-04-20')
    , ('polkadot', '1xpD24SQ9UgeFPQ2P9eRc7dppjgU9hiHDULqvWNfH3g3U54', 'OKX', 'OKX 3', 'hildobby', date '2024-04-20')
    , ('polkadot', '16hp43x8DUZtU8L3cJy9Z8JMwTzuu8ZZRWqDZnpMhp464oEd', 'OKX', 'OKX 4', 'hildobby', date '2024-04-20')
    , ('polkadot', '1Dkx7zjy4pRMwLQwWkbhb9Jxy7EXLfkdHVRXufwvdLV73QV', 'OKX', 'OKX 5', 'hildobby', date '2024-04-20')
    , ('polkadot', '1xXbYy1V5Sc3EQZ76wmcWy4gXTSyLbzgdDNJtGT6jEcL2z7', 'Swissborg', 'Swissborg 1', 'hildobby', date '2024-04-20')
    , ('polkadot', '15Fg7p6pzLo6uinCFdsx3HTWdAx4vFt8nnw2E3JWHHwh9NCn', 'Swissborg', 'Swissborg 2', 'hildobby', date '2024-04-20')
    , ('polkadot', '15mENJiKxtbxE2PNcB8qTaatYKjFTN4kitEzZ5eiHFGW3DVU', 'Swissborg', 'Swissborg 3', 'hildobby', date '2024-04-20')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
