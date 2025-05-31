{{config(
        tags = ['static'],
        schema = 'cex_near',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["near"]\',
                                    "sector",
                                    "cex",
                                    \'["Sector920"]\') }}')}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('near', '9b23bfd45a028e7a28326007b221cf8bd8b288630dde5ca714a0524f4865db89', 'swissborg', 'swissborg hot wallet 1', 'Sector920', date '2025-05-20')
    , ('near', '1c0dc521827821f1695538ff9312df15f32837bbfe95bc937f9faaafd4125552', 'swissborg', 'swissborg hot_wallet 2', 'Sector920', date '2025-05-20')
    , ('near', 'dc6b4c8821dbe652b763e3bfadfea548137b29b97408c8b411ad72acff94e63f', 'swissborg', 'swissborg hot_wallet 3', 'Sector920', date '2025-05-20')
    , ('near', 'sborgsa.near', 'swissborg', 'swissborg', 'Sector920', date '2025-05-20')
    , ('near', '30d6205b89e02d2c34ade165ae9705f8d9e80d0e304061cb10b8e56c8e703f62', 'okx', 'okx hot wallet 1', 'Sector920', date '2025-05-20')
    , ('near', '3e5be3324ae2b6459383812c09e404452c034306f51ed680f7ce374ef8697846', 'okx', 'okx hot wallet 2', 'Sector920', date '2025-05-20')
    , ('near', 'd73888a2619c7761735f23c798536145dfa87f9306b5f21275eb4b1a7ba971b9', 'okx', 'okx hot wallet 3', 'Sector920', date '2025-05-20')
    , ('near', '423df0a6640e9467769c55a573f15b9ee999dc8970048959c72890abf5cc3a8e', 'mexc', 'mexc hot wallet 1', 'Sector920', date '2025-05-20')
    , ('near', '2c4106971c0d9d4b7e9e05a6efab71de4be77c0b023af5ec4fde46fb796fb9b0', 'mexc', 'mexc hot wallet 2', 'Sector920', date '2025-05-20')
    , ('near', '5623c9fbb2f1b6b12ba775031dbf52fab977a8b466b64f153f3743069e13bb34', 'kucoin', 'kucoin hot wallet 1', 'Sector920', date '2025-05-20')
    , ('near', 'eb3c2bfd6bf357f433be30292d78b23f948ee75f63baca2cc5f56cff1751c294', 'kraken', 'kraken hot wallet 2', 'Sector920', date '2025-05-20')
    , ('near', '6773ab0dd6c75d420c0f9393fe2112ec91a9843bda9bc84d3ba87fc4e40a747b', 'huobi', 'huobi', 'Sector920', date '2025-05-20')
    , ('near', '45c4b86a07f44cfce03a1697f121e1ac4a078bb72dbe590f7c838819561fe0c4', 'huobi', 'huobi hot wallet 1', 'Sector920', date '2025-05-20')
    , ('near', '601483a1b22699b636f1df800b9b709466eba4e1d5ce7c2e1e20317af8bbd1f3', 'huobi', 'huobi hot wallet 2', 'Sector920', date '2025-05-20')
    , ('near', '0d584a4cbbfd9a4878d816512894e65918e54fae13df39a6f520fc90caea2fb0', 'gate.io', 'gate.io hot wallet 1', 'Sector920', date '2025-05-20')
    , ('near', '25ab96ddd0d9a8e0ab48b45c2ef7473a83c4ff7e49239b46613b3d688e024731', 'coinbase', 'coinbase hot wallet 1', 'Sector920', date '2025-05-20')
    , ('near', 'ccb91e1db61e8d7e1d4ae3e043001140132959a86ee35a548b6563a46284a6ea', 'bybit', 'bybit hot wallet', 'Sector920', date '2025-05-20')
    , ('near', '7e5c96c615716914624c23ce307c9d1d6dd651510cecf4d005784ef2db8c6063', 'bitz', 'bitz hot wallet', 'Sector920', date '2025-05-20')
    , ('near', '965c8b57b7f36fe9a40ce6188d444ca78708e4d83843b108f7f53d0fe5332076', 'bitfinex', 'bitfinex hot wallet 1', 'Sector920', date '2025-05-20')
    , ('near', '383e50ea1a754ed3acd0d59116f221add87adb82559f31ca6d377f058fe83375', 'bitfinex', 'bitfinex hot wallet 2', 'Sector920', date '2025-05-20')
    , ('near', '5c33c6218d47e00ef229f60da78d0897e1ee9665312550b8afd5f9c7bc6957d2', 'binance', 'binance hot wallet 1', 'Sector920', date '2025-05-20')
    , ('near', '7747991786f445efb658b69857eadc7a57b6b475beec26ed14da8bc35bb2b5b6', 'binance', 'binance hot wallet 2', 'Sector920', date '2025-05-20')
    , ('near', 'binancecold3.near', 'binance', 'binance cold wallet 1', 'Sector920', date '2025-05-20')
    , ('near', 'binance1.near', 'binance', 'binance cold wallet 2', 'Sector920', date '2025-05-20')
) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date);
