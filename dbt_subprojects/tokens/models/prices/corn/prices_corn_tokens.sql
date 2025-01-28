{% set blockchain = 'corn' %}

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
    ('wbtc-wrapped-bitcoin', 'wBTCN', 0xda5ddd7270381a7c2717ad10d1c0ecb19e3cdfb2, 18)
    , ('lbtc-lombard-staked-btc', 'LBTC', 0xecAc9C5F704e954931349Da37F60E39f515c11c1, 8)
    , ('usdce-usd-coine', 'USDC.e', 0xDF0B24095e15044538866576754F3C964e902Ee6, 6)
    , ('pumpbtc-pumpbtc', 'pumpBTC', 0xF469fBD2abcd6B9de8E169d128226C0Fc90a012e, 8)
) as temp (token_id, symbol, contract_address, decimals)
