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
    ('usdc-usd-coin', 'linea', 'USDC', 0x176211869cA2b568f2A7D4EE941E073a821EE1ff, 6)
    ,('usdt-tether', 'linea', 'USDT', 0xA219439258ca9da29E9Cc4cE5596924745e12B93, 6)
    ,('weth-weth', 'linea', 'WETH', 0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f, 18)
    ,('wsteth-wrapped-liquid-staked-ether-20', 'linea', 'wstETH', 0xB5beDd42000b71FddE22D3eE8a79Bd49A568fC8F, 18)
    ,('shib-shiba-inu', 'linea', 'SHIB', 0x99AD925C1Dc14Ac7cc6ca1244eeF8043C74E99d5, 18)
    ,('wbtc-wrapped-bitcoin', 'linea', 'WBTC', 0x3aAB2285ddcDdaD8edf438C1bAB47e1a9D05a9b4, 8)
    ,('uni-uniswap', 'linea', 'UNI', 0x636B22bC471c955A8DB60f28D4795066a8201fa3, 18)
    ,('link-chainlink', 'linea', 'LINK', 0x5B16228B94b68C7cE33AF2ACc5663eBdE4dCFA2d, 18)
    ,('dai-dai', 'linea', 'DAI', 0x4AF15ec2A0BD43Db75dd04E62FAA3B8EF36b00d5, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
