{% set blockchain = 'monad' %}

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
      ('usdc-usd-coin', 'USDC', 0x754704Bc059F8C67012fEd69BC8A327a5aafb603, 6)
    , ('ausd-agora-dollar', 'AUSD', 0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a, 6)
    , ('weth-weth', 'WETH', 0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242, 18)
    , ('sol-solana', 'WSOL', 0xea17E5a9efEBf1477dB45082d67010E2245217f1, 9)
) as temp (token_id, symbol, contract_address, decimals)
