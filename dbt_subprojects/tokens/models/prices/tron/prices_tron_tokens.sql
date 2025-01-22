{% set blockchain = 'tron' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static']
    )
}}

SELECT
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , from_tron_address(contract_address) as contract_address
    , decimals
FROM
(
    VALUES
    ('trx-tron', 'WTRX', 'TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR', 6)
    , ('usdt-tether', 'USDT', 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 6)
    , ('weth-weth', 'ETHB', 'TRFe3hT5oYhjSZ6f3ji5FJ7YCfrkWnHRvh', 18)
    , ('usdc-usdc', 'USDCOLD', 'TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8', 6)
    , ('doge-dogecoin', 'DOGE', 'THbVQp8kMjStKNnf2iCY6NEzThKMK5aBHg', 8)
    , ('weth-weth', 'WETH', 'TXWkP3jLBqRGojUih1ShzNyDaN5Csnebok', 18)
    , ('ltc-litecoin', 'LTC', 'TR3DLthpnDdCGabhVDbD3VMsiJoCXY3bZd', 8)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 'TXpw8XeWYeTUd4quDskoUqeQPowRh4jY65', 8)
    , ('tusd-trueusd', 'TUSD', 'TUpMhErZL2fhh4sVNULAbNKLokS4GjC1F4', 18)
    , ('htx-htx', 'HTX', 'TUPM7K8REVzD2UdV4R5fe5M8XbnR2DdoJ6', 18)
    , ('jst-just', 'JST', 'TCFLL5dx5ZJdKnWuesXxi1VPwjLVmWZZy9', 18)
    , ('wbt-whitebit-coin', 'WBT', 'TFptbWaARrWTX5Yvy3gNG5Lm8BmhPx82Bt', 8)
    , ('btt-bittorrent', 'BTT', 'TAFjULxiVgT4qWk6UZwjqwZXTSaGaqnVp4', 18)
    , ('usdd-usdd', 'USDD', 'TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn', 18)
) as temp (token_id, symbol, contract_address, decimals)
