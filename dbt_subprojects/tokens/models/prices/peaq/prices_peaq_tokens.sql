{% set blockchain = 'peaq' %}

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
    ('usdt-tether', 'USDT', 0xf4D9235269a96aaDaFc9aDAe454a0618eBE37949, 6)
    , ('usdc-usd-coin', 'USDC', 0xbbA60da06c2c5424f03f7434542280FCAd453d10, 6)
    , ('peaq-peaq-network', 'wPEAQ', 0xE5330a9fBA99504C534127D39727729899c9a506, 18)
) as temp (token_id, symbol, contract_address, decimals) 