{{config(
        tags = ['static'],
        schema = 'cex_cosmos',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["cosmos"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('cosmos', 'cosmos1h9ymfm2fxrqgd257dlw5nku3jgqjgpl59sm5n', 'Bitfinex', 'Bitfinex 1', 'hildobby', date '2024-04-20')
    , ('cosmos', 'cosmos1jtdkj8hxhj88jxv8lul9xvdpnwsl00evvvpnh', 'Bitfinex', 'Bitfinex 2', 'hildobby', date '2024-04-20')
    , ('cosmos', 'cosmos12chvl78ffgvzc29mvrg5auz94vgksne5svsje', 'Coinsquare', 'Coinsquare 1', 'hildobby', date '2024-04-20')
    , ('cosmos', 'cosmos1wt5sdluapdqrp8wljyesl7s3x5vzq5z76t4nu', 'LAToken', 'LAToken 1', 'hildobby', date '2024-04-20')
    , ('cosmos', 'cosmos10dfzd2wpnpeuy2lgan35ah8dg5p4l298v0n8e', 'Swissborg', 'Swissborg 1', 'hildobby', date '2024-04-20')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
