{% set blockchain = 'ronin' %}

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
    ('ron-ronin-token', 'WRON', 0xe514d9deb7966c8be0ca922de8a064264ea6bcd4, 18)
    , ('weth-weth', 'WETH', 0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5, 18)
    , ('usdc-usd-coin', 'USDC', 0x0b7007c13325c48911f73a2dad5fa5dcbf808adc, 6)
    , ('axs-axie-infinity', 'AXS', 0x97a9107c1793bc407d6f527b77e7fff4d812bece, 18)
    , ('slp-smooth-love-potion', 'SLP', 0xa8754b9fa15fc18bb59458815510e40a12cd2014, 0)
) as temp (token_id, symbol, contract_address, decimals)