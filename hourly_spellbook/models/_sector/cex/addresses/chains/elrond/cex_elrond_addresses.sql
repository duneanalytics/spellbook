{{config(
        tags = ['static'],
        schema = 'cex_elrond',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["elrond"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('elrond', 'erd1a56dkgcpwwx6grmcvw9w5vpf9zeq53w3w7n6dmxcpxjry3l7uh2s3h9dtr', 'Bitfinex', 'Bitfinex 1', 'hildobby', date '2024-04-20')
    , ('elrond', 'erd1z5xjeu4xw32jkckhj9jpc9dymj6a9h8yxtch96e43ncp6fhuzpnqshqutj', 'LAToken', 'LAToken 1', 'hildobby', date '2024-04-20')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
