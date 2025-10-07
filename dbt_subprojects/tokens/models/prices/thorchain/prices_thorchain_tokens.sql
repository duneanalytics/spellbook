{% set blockchain = 'thorchain' %}

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
    , cast(contract_address as varbinary) as contract_address
    , contract_address as contract_address_full
    , decimals
FROM
(
    VALUES
    ('rune-thorchain', 'RUNE', 'THOR.RUNE', 8)
    , ('btc-bitcoin', 'BTC', 'BTC.BTC', 8)
    , ('btc-bitcoin', 'BTC', 'BTC/BTC', 8)  
    , ('btc-bitcoin', 'BTC', 'BTC~BTC', 8)
    , ('eth-ethereum', 'ETH', 'ETH/ETH', 18)
    , ('eth-ethereum', 'ETH', 'ETH.ETH', 18)
    , ('eth-ethereum', 'ETH', 'ETH~ETH', 18)
    , ('usdt-tether', 'USDT', 'ETH/USDT-0XDAC17F958D2EE523A2206206994597C13D831EC7', 6)
    , ('usdt-tether', 'USDT', 'ETH~USDT-0XDAC17F958D2EE523A2206206994597C13D831EC7', 6)
    , ('usdt-tether', 'USDT', 'ETH.USDT-0XDAC17F958D2EE523A2206206994597C13D831EC7', 6)
    , ('usdt-tether', 'USDT', 'BSC.USDT-0X55D398326F99059FF775485246999027B3197955', 18)
    , ('usdt-tether', 'USDT', 'BSC~USDT-0X55D398326F99059FF775485246999027B3197955', 18)
    , ('usdc-usd-coin', 'USDC', 'ETH/USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48', 6)
    , ('usdc-usd-coin', 'USDC', 'ETH~USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48', 6)
    , ('busd-binance-usd', 'BUSD', 'BNB/BUSD-BD1', 8)
    , ('busd-binance-usd', 'BUSD', 'BNB.BUSD-BD1', 8)
    , ('bnb-binance-coin', 'BNB', 'BSC.BNB', 18)
    , ('bnb-binance-coin', 'BNB', 'BNB.BNB', 8)
    , ('bnb-binance-coin', 'BNB', 'BNB/BNB', 8)
    , ('doge-dogecoin', 'DOGE', 'DOGE/DOGE', 8)
) as temp (token_id, symbol, contract_address, decimals)
