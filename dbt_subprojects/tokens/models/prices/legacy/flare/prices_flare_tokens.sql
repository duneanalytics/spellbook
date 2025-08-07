{% set blockchain = 'flare' %}

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
    ('flr-flare-network', 'WFLR', 0x1D80c49BbBCd1C0911346656B529DF9E5c2F783d, 18)
    , ('joule-kinetic', 'JOULE', 0xE6505f92583103AF7ed9974DEC451A7Af4e3A3bE, 18)
    , ('usdc.e-usd-coin.e', 'USDC.e', 0xFbDa5F676cB37624f28265A144A48B0d6e87d3b6, 6)
    , ('usdt-tether', 'USDT', 0x0B38e83B86d491735fEaa0a791F65c2B99535396, 6)
) as temp (token_id, symbol, contract_address, decimals)
