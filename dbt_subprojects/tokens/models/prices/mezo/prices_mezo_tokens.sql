{% set blockchain = 'mezo' %}

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
      ('usdc-usd-coin', 'MUSD', 0xdd468a1ddc392dcdbef6db6e34e89aa338f9f186, 18)
    , ('usdc-usd-coin', 'mUSDC', 0x04671c72aab5ac02a03c1098314b1bb6b560c197, 6)
    , ('usdt-tether', 'mUSDT', 0xeb5a5d39de4ea42c2aa6a57eca2894376683bb8e, 6)
) as temp (token_id, symbol, contract_address, decimals)
