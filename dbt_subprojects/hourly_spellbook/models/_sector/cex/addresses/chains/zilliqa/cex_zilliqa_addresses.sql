{{config(
        tags = ['static'],
        schema = 'cex_zilliqa',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["zilliqa"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('zilliqa', 'zil1xfsrre5qgx0mqg99xc0l2cuyu9ntt259ngsu7s', 'Bitfinex', 'Bitfinex 1', 'hildobby', date '2024-04-20')
    , ('zilliqa', 'zil184u2al6n0nrks06xjgq080hc95f77ttd7rkqvn', 'Bitfinex', 'Bitfinex 2', 'hildobby', date '2024-04-20')
    , ('zilliqa', 'zil1rklazrfy5spul4tqzc2jqfvuneszcjrdya6a8y', 'LAToken', 'LAToken 1', 'hildobby', date '2024-04-20')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
