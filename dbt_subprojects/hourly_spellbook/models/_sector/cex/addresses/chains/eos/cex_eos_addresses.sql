{{config(
        tags = ['static'],
        schema = 'cex_eos',
        alias = 'addresses'
        , post_hook='{{ hide_spells() }}'
    )
}}

SELECT blockchain, address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    ('eos', 'bitfinexdep1', 'Bitfinex', 'Bitfinex 8', 'hildobby', date '2024-04-20')
    , ('eos', 'bitfinexcw21', 'Bitfinex', 'Bitfinex 9', 'hildobby', date '2024-04-20')
    , ('eos', 'bitfinexcw23', 'Bitfinex', 'Bitfinex 10', 'hildobby', date '2024-04-20')
    , ('eos', 'bitfinexcw24', 'Bitfinex', 'Bitfinex 11', 'hildobby', date '2024-04-20')
    , ('eos', 'bitfinexcw25', 'Bitfinex', 'Bitfinex 12', 'hildobby', date '2024-04-20')
    , ('eos', 'bitfinexcw31', 'Bitfinex', 'Bitfinex 13', 'hildobby', date '2024-04-20')
    , ('eos', 'bitfinexcw22', 'Bitfinex', 'Bitfinex 14', 'hildobby', date '2024-04-20')
    , ('eos', 'bitfinexcw33', 'Bitfinex', 'Bitfinex 15', 'hildobby', date '2024-04-20')
    , ('eos', 'bitfinexcw55', 'Bitfinex', 'Bitfinex 16', 'hildobby', date '2024-04-20')
    , ('eos', 'vuniyuoxoeub', 'HTX', 'HTX 7', 'hildobby', date '2024-04-20')
    , ('eos', 'hotbitioeoss', 'Hotbit', 'Hotbit 3', 'hildobby', date '2024-04-20')
    , ('eos', 'hotbitioeos2', 'Hotbit', 'Hotbit 4', 'hildobby', date '2024-04-20')
    , ('eos', 'qlwzviixzm1h', 'KuCoin', 'KuCoin 104', 'hildobby', date '2024-04-20')
    , ('eos', 'kucoinrise11', 'KuCoin', 'KuCoin 105', 'hildobby', date '2024-04-20')
    , ('eos', 'kucoinatwarm', 'KuCoin', 'KuCoin 106', 'hildobby', date '2024-04-20')
    , ('eos', 'latokenabbc1', 'LAToken', 'LAToken 9', 'hildobby', date '2024-04-20')
    , ('eos', 'latokeneos11', 'LAToken', 'LAToken 10', 'hildobby', date '2024-04-20')
    ) AS x (blockchain, address, cex_name, distinct_name, added_by, added_date)
