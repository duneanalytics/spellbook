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
    ,('nile-nile', 'linea', 'NILE', 0xaaaac83751090c6ea42379626435f805ddf54dc8, 18)
    ,('zero-zerolend', 'linea', 'ZERO', 0x78354f8dccb269a615a7e0a24f9b0718fdc3c7a7, 18)
    ,('weeth-bridged-weeth-linea', 'linea', 'weETH', 0x1bf74c010e6320bab11e2e5a532b5ac15e0b8aa6, 18)
    ,('foxy-foxy', 'linea', 'FOXY', 0x5fbdf89403270a1846f5ae7d113a989f850d1566, 18)
    ,('wrseth-wrapped-rseth', 'linea', 'wrsETH', 0xd2671165570f41bbb3b0097893300b6eb6101e6c, 18)
    ,('mendi-mendi-finance', 'linea', 'MENDI', 0x43e8809ea748eff3204ee01f08872f063e44065f, 18)
    ,('croak-croak_on_linea', 'linea', 'CROAK', 0xacb54d07ca167934f57f829bee2cc665e1a5ebef, 18)
    ,('linda-linda-2', 'linea', 'LINDA', 0x82cc61354d78b846016b559e3ccd766fa7e793d5, 18)
    ,('mkr-linea-bridged-mkr-linea', 'linea', 'MKR', 0x2442bd7ae83b51f6664de408a385375fe4a84f52, 18)
    ,('pepe-linea-bridged-pepe-linea', 'linea', 'PEPE', 0x7da14988e4f390c2e34ed41df1814467d3ade0c3, 18)
    ,('rseth-layerzero-bridged-rseth-linea', 'linea', 'rsETH', 0x4186bfc76e2e237523cbc30fd220fe055156b41f, 18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
