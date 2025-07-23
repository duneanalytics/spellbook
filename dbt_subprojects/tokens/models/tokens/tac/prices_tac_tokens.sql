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
    ('tac-tac-protocol', 'TAC', 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, 18)
    , ('toncoin-the-open-network', 'TON', 0xb76d91340f5ce3577f0a056d29f6e3eb4e88b140, 9)
    , ('usdt-tether', 'USDT', 0xaf988c3f7cb2aceabb15f96b19388a259b6c438f, 6)
    , ('cbbtc-coinbase-wrapped-btc', 'cbBTC', 0x7048c9e4abd0cf0219e95a17a8c6908dfc4f0ee4, 8)
    , ('weth-weth', 'WETH', 0x61d66bc21fed820938021b06e9b2291f3fb91945, 18)
    , ('lbtc-lombard-staked-btc', 'LBTC', 0xecac9c5f704e954931349da37f60e39f515c11c1, 8)
    , ('wsteth-wrapped-liquid-staked-ether-20', 'WSTETH', 0xaf368c91793cb22739386dfcbbb2f1a9e4bcbebf, 18)
    , ('wrseth-wrapped-rseth', 'wrsETH', 0x5448bbf60ee2edbcd32f032f3294982f4ad1119e, 18)
) as temp (token_id, symbol, contract_address, decimals)
