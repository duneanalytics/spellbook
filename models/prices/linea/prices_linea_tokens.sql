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
    ,('m-btc-merlins-seal-btc', 'linea', 'M-BTC',0xe4D584ae9b753e549cAE66200A6475d2f00705f7, 18)
    ,('solvbtc-solv-protocol-solvbtc', 'linea', 'solvBTC', 0x5FFcE65A40f6d3de5332766ffF6A28BF491C868c, 18)
    ,('lynx2-lynex', 'linea', 'LYNX', 0x1a51b19ce03dbe0cb44c1528e34a7edd7771e9af, 18)   
    ,('usdplus-usdplus', 'linea', 'USD+', 0xb79dd08ea68a908a97220c76d19a6aa9cbde4376, 6)
    ,('ezeth-renzo-restaked-eth', 'linea', 'ezETH', 0x2416092f143378750bb29b79ed961ab195cceea5, 18)
    ,('usde-ethena-usde', 'linea', 'USDe', 0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
