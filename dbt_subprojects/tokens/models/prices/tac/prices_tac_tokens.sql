{% set blockchain = 'tac' %}

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
    (('wtac-wrapped-tac', 'WTAC', 0x07840B012d84095397Fd251Ea619cee6F866bC39, 18)
    , ('ton-ton-token', 'TON', 0xbE3C16e14d578a24eF4B124fAf9CD1bb5F1e964B, 9)
    , ('usdt-tether-usd', 'USDT', 0xd7d11F0CA352a83aB11fAed983Cf28ACB5AB61aD, 6)
) as temp (token_id, symbol, contract_address, decimals)
