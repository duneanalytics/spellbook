{{ config(
        schema='prices_plume',
        alias='tokens',
        materialized='table',
        file_format='delta',
        tags=['static']
        )
}}

SELECT
    token_id,
    blockchain,
    symbol,
    contract_address,
    decimals
FROM
(
    VALUES
    ('plume-plume', 'plume', 'WPLUME', 0xEa237441c92CAe6FC17Caaf9a7acB3f953be4bd1, 18),
    ('usdt-tether', 'plume', 'USDT', 0xda6087E69C51E7D31b6DBAD276a3c44703DFdCAd, 6),
    ('weth-weth', 'plume', 'pETH', 0x39d1F90eF89C52dDA276194E9a832b484ee45574, 18),
    ('weth-weth', 'plume', 'WETH', 0xca59cA09E5602fAe8B629DeE83FfA819741f14be, 18),
    ('usdc-usd-coin', 'plume', 'USDC.e', 0x78adD880A697070c1e765Ac44D65323a0DcCE913, 6)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
