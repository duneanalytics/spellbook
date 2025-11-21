{% set blockchain = 'superseed' %}

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
    , contract_address
    , decimals
FROM
(
    VALUES
    ('weth-weth', 'WETH', 0x4200000000000000000000000000000000000006, 18)
    , ('usdc-usd-coin', 'USDC', 0xc316c8252b5f2176d0135ebb0999e99296998f2e, 6)
    , ('ousdt-openusdt', 'oUSDT', 0x1217BfE6c773EEC6cc4A38b5Dc45B92292B6E189, 6)
    , ('usdt-tether', 'USDT', 0xc5068BB6803ADbe5600DE5189fe27A4dAcE31170, 6)
    , ('cbbtc-coinbase-wrapped-btc', 'cbBTC', 0x6f36DBD829DE9b7e077DB8A35b480d4329ceB331, 8)
    , ('supr-superseed', 'SUPR', 0x6EA1fFcbD7F5D210dB07D9E773862B0512fA219B, 18)
) as temp (token_id, symbol, contract_address, decimals)
