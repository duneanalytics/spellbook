{{ config(
        schema='prices_linea',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}
SELECT
    token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES

    ('usdc-usd-coin', 'linea', 'USDC', 0x176211869cA2b568f2A7D4EE941E073a821EE1ff, 6),
    ('usdt-tether', 'linea', 'USDT', 0xA219439258ca9da29E9Cc4cE5596924745e12B93, 6),
    ('weth-weth', 'linea', 'WETH', 0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f, 18),
    ('wsteth-wrapped-liquid-staked-ether-20', 'linea', 'wstETH', 0xB5beDd42000b71FddE22D3eE8a79Bd49A568fC8F, 18),

) as temp (token_id, blockchain, symbol, contract_address, decimals)
