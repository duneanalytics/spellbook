{% set blockchain = 'tempo' %}

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
    ('usdc-usd-coin', 'USDC.e', 0x20c000000000000000000000b9537d11c60e8b50, 6)
    , ('euroc-euro-coin', 'EURC.e', 0x20c0000000000000000000001621e21f71cf12fb, 6)
    , ('usdt0-usdt0', 'USDT0', 0x20c00000000000000000000014f22ca97301eb73, 6)
    , ('frxusd-frax-usd', 'frxUSD', 0x20c0000000000000000000003554d28269e0f3c2, 6)
    , ('cusd-cap-usd', 'cUSD', 0x20c0000000000000000000000520792dcccccccc, 6)
    -- , ('stcusd-staked-cap-usd', 'stcUSD', 0x20c0000000000000000000008ee4fcff88888888, 6) --inactive on Coinpaprika
    -- , ('', 'pathUSD', 0x20c0000000000000000000000000000000000000, 6) --not available on Coinpaprika
) as temp (token_id, symbol, contract_address, decimals)
