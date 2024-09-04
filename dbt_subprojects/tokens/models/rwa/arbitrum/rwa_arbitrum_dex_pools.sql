
{{
    config(
        schema = 'rwa_arbitrum'
        ,alias = 'dex_pools'
        ,materialized = 'table'
        ,post_hook='{{ expose_spells(\'["arbitrum"]\',
                        "sector",
                        "rwa",
                        \'["maybeYonas", "pyor_xyz"]\') }}'
    )
}}

select
    project,
    version,
    pool,
    token_address,
    token_is_rwa,
    symbol,
    protocol,
    type
from (values
    ('curve', 'stableswap-ng', 0x4bD135524897333bec344e50ddD85126554E58B4, 0xaf88d065e77c8cC2239327C5EDb3A432268e5831, 0, 'USDC', 'Mountain Protocol', 'RWA'),
    ('curve', 'stableswap-ng', 0x4bD135524897333bec344e50ddD85126554E58B4, 0x59D9356E565Ab3A36dD77763Fc0d87fEaf85508C, 0, 'USDM', 'Mountain Protocol', 'RWA'),
    ('uniswap', '3', 0xfEAA137F43f88b7F767F5A67978FfF8EC11Cc6Ef, 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1, 0, 'WETH', 'Frax Finance', 'Governance'),
    ('uniswap', '3', 0xfEAA137F43f88b7F767F5A67978FfF8EC11Cc6Ef, 0x9d2F299715D94d8A7E6F5eaa8E654E8c74a988A7, 0, 'FXS', 'Frax Finance', 'Governance'),
    ('ramses', '3', 0xa45Cbd521ED745F6C856C89F8dBB583bD1591fC2, 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1, 0, 'WETH', 'Frax Finance', 'Governance'),
    ('ramses', '3', 0xa45Cbd521ED745F6C856C89F8dBB583bD1591fC2, 0x9d2F299715D94d8A7E6F5eaa8E654E8c74a988A7, 0, 'FXS', 'Frax Finance', 'Governance')
) as t(
    project,
    version,
    pool,
    token_address,
    token_is_rwa,
    symbol,
    protocol,
    type
)
