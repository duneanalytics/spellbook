{% set blockchain = 'apechain' %}

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
    ('ape-apecoin', 'WAPE', 0x48b62137edfa95a428d35c09e44256a739f6b557, 18)
    , ('gns-gains-network', 'GNS', 0xe31c676d8235437597581b44c1c4f8a30e90b38a, 18)
    , ('ape-apecoin', 'APE', 0xfc7b0badb1404412a747bc9bb6232e25098be303, 18)
    , ('eth-ethereum', 'ApeETH', 0xcf800f4948d16f23333508191b1b1591daf70438, 18)
    , ('usdc-usd-coin', 'ApeUSD', 0xa2235d059f80e176d931ef76b6c51953eb3fbef4, 18)
) as temp (token_id, symbol, contract_address, decimals)
