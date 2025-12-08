{% set blockchain = 'xlayer' %}

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
    -- placeholder rows, add actual tokens as needed
    ('okb-okb', 'OKB', 0xe538905cf8410324e03a5a23c1c177a474d59b2b, 18) -- WOKB
    , ('weth-weth', 'WETH', 0x5a77f1443d16ee5761d310e38b62f77f726bc71c, 18)
    , ('usdt-tether', 'USDT', 0x1e4a5963abfd975d8c9021ce480b42188849d41d, 6)
    , ('usdt-tether', 'USDT', 0x779ded0c9e1022225f8e0630b35a9b54be713736, 6) -- USDT0
    , ('usdc-usd-coin', 'USDC', 0x74b7f16337b8972027f6196a17a631ac6de26d22, 6)
    , ('usdce-usd-coine', 'USDC.e', 0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035, 6)
    , ('wbtc-wrapped-bitcoin', 'WBTC', 0xea034fb02eb1808c2cc3adbc15f447b93cbe08e1, 8)
    , ('dai-dai', 'DAI', 0xc5015b9d9161dca7e18e32f6f25c4ad850731fd4, 18)
) as temp (token_id, symbol, contract_address, decimals)
