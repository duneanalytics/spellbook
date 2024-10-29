{{
    config(
        schema = 'tokens_tron'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}


SELECT
    , from_tron_address(contract_address) as contract_address
    , symbol
    , decimals
FROM
(
    VALUES
    ('WTRX', 'TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR', 6)
    , ('USDT', 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 6)
    , ('ETHB', 'TRFe3hT5oYhjSZ6f3ji5FJ7YCfrkWnHRvh', 18)
    , ('USDCOLD', 'TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8', 6)
    , ('DOGE', 'THbVQp8kMjStKNnf2iCY6NEzThKMK5aBHg', 8)
    , ('WETH', 'TXWkP3jLBqRGojUih1ShzNyDaN5Csnebok', 18)
    , ('LTC', 'TR3DLthpnDdCGabhVDbD3VMsiJoCXY3bZd', 8)
    , ('WBTC', 'TXpw8XeWYeTUd4quDskoUqeQPowRh4jY65', 8)
    , ('TUSD', 'TUpMhErZL2fhh4sVNULAbNKLokS4GjC1F4', 18)
    , ('HTX', 'TUPM7K8REVzD2UdV4R5fe5M8XbnR2DdoJ6', 18)
    , ('JST', 'TCFLL5dx5ZJdKnWuesXxi1VPwjLVmWZZy9', 18)
    , ('WBT', 'TFptbWaARrWTX5Yvy3gNG5Lm8BmhPx82Bt', 8)
    , ('BTT', 'TAFjULxiVgT4qWk6UZwjqwZXTSaGaqnVp4', 18)
    , ('USDD', 'TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn', 18)
) as temp (symbol, contract_address, decimals)
