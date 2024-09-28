{{config(
        tags = ['static'],
        schema = 'cex_aptos',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["aptos"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('aptos', '0x5bd7de5c56d5691f32ea86c973c73fec7b1445e59736c97158020018c080bb00', 'Binance', 'Binance 1', 'hildobby', date '2024-04-20')
    , ('aptos', '0x80174e0fe8cb2d32b038c6c888dd95c3e1560736f0d4a6e8bed6ae43b5c91f6f', 'Binance', 'Binance 2', 'hildobby', date '2024-04-20')
    , ('aptos', '0xae1a6f3d3daccaf77b55044cea133379934bba04a11b9d0bbd643eae5e6e9c70', 'Binance', 'Binance 3', 'hildobby', date '2024-04-20')
    , ('aptos', '0xd91c64b777e51395c6ea9dec562ed79a4afa0cd6dad5a87b187c37198a1f855a', 'Binance', 'Binance 4', 'hildobby', date '2024-04-20')
    , ('aptos', '0xed8c46bec9dbc2b23c60568f822b95b87ea395f7e3fdb5e3adc0a30c55c0a60e', 'Binance', 'Binance 5', 'hildobby', date '2024-04-20')
    , ('aptos', '0x0213c67ed78bc280887234fe5ed5e77272465317978ae86c25a71531d9332a2d', 'Binance', 'Binance 6', 'hildobby', date '2024-04-20')
    , ('aptos', '0xfd9192f8ad8dc60c483a884f0fbc8940f5b8618f3cf2bbf91693982b373dfdea', 'Bitfinex', 'Bitfinex 1', 'hildobby', date '2024-04-20')
    , ('aptos', '0x834d639b10d20dcb894728aa4b9b572b2ea2d97073b10eacb111f338b20ea5d7', 'OKX', 'OKX 1', 'hildobby', date '2024-04-20')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
