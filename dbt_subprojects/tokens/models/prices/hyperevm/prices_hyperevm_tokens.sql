{% set blockchain = 'hyperevm' %}

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
    ('usdt-tether', 'USDT0', 0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb, 6)
    , ('hype-hyperliquid', 'WHYPE', 0x5555555555555555555555555555555555555555, 18)
    , ('eth-ethereum', 'UETH', 0xbe6727b535545c67d5caa73dea54865b92cf7907, 18)
    , ('btc-bitcoin', 'UBTC', 0x9fdbda0a5e284c32744d2f17ee5c74b284993463, 8)
    , ('pump-pumpfun', 'UPUMP', 0x27ec642013bcb3d80ca3706599d3cda04f6f4452, 18)
    , ('sol-solana', 'USOL', 0x068f321fa8fb9f0d135f290ef6a3e2813e1c8a29, 9)
) as temp (token_id, symbol, contract_address, decimals)