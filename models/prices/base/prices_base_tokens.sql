{{ config(
        schema='prices_base',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags=['static']
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
    ('weth-weth','base','WETH',0x4200000000000000000000000000000000000006,18),
    ('axl-axelar','base','AXL',0x23ee2343b892b1bb63503a4fabc840e0e2c6810f,6),
    ('bald-bald','base','BALD',0x27d2decb4bfc9c76f0309b8e88dec3a601fe25a8,18),
    ('usdbc-usd-base-coin','base','USDbC',0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca,6),
    ('axlusdc-axelar-wrapped-usdc','base','axlUSDC',0xeb466342c4d449bc9f53a865d5cb90586f405215,6),
    ('dai-dai','base','DAI',0x50c5725949a6f0c72e6c4a641f24049a917db0cb,18),
    ('cbeth-coinbase-wrapped-staked-eth','base','cbETH',0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22,18),
    ('mim-magic-internet-money','base','MIM',0x4a3a6dd60a34bb2aba60d73b4c88315e9ceb6a3d,18),
    ('axlusdt-axelar-usd-tether','base','axlUSDT',0x7f5373ae26c3e8ffc4c77b7255df7ec1a9af52a6,6),
    ('boost-perpboost','base','BOOST',0x71e8f538f47397cd9a544041555cafc7a0ce9ae3,18),
    ('based-basedmarkets','base','BASED',0xba5e6fa2f33f3955f0cef50c63dcc84861eab663,18),
    ('usdc-usd-coin', 'base', 'USDC', 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913, 6),
    ('wsteth-wrapped-liquid-staked-ether-20', 'base', 'wstETH',0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452,18),
    ('aero-aerodrome-finance', 'base', 'AERO', 0x940181a94A35A4569E4529A3CDfB74e38FD98631, 18),
    ('degen-degen-base', 'base', 'DEGEN', 0x4ed4e862860bed51a9570b96d89af5e1b0efefed,18),
    ('dai-plus-overnight', 'base', 'DAI+', 0x65a2508c429a6078a7bc2f7df81ab575bd9d9275,18),
    ('usd-plus-overnight', 'base', 'USD+', 0xb79dd08ea68a908a97220c76d19a6aa9cbde4376,18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
