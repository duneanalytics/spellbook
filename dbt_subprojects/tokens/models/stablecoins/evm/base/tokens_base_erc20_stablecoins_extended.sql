{% set chain = 'base' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_extended',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- extended list: new stablecoin addresses added after the core list was frozen
-- add new stablecoins here (not in tokens_base_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address
from (values

     (0x0dc4f92879b7670e5f4e4e6e3c801d229129d90d), -- wARS
     (0x337e7456b420bd3481e7fa61fa9850343d610d34), -- wMXN
     (0xd76f5faf6888e24d9f04bf92a0c8b921fe4390e0), -- wBRL
     (0x449b3317a6d1efb1bc3ba0700c9eaa4ffff4ae65), -- AUDD
     (0x4933a85b5b5466fbaf179f72d3de273c287ec2c2), -- EURAU
     (0xfb8718a69aed7726afb3f04d2bd4bfde1bdcb294), -- TRYB
     (0x0a4c9cb2778ab3302996a34befcf9a8bc288c33b), -- XSGD
     (0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553)  -- ZCHF

     /* rebasing / interest accruing tokens
     (0x526728dbc96689597f85ae4cd716d4f7fccbae9d), -- msUSD (morpho)
     (0x4e65fe4dba92790696d040ac24aa414708f5c0ab)  -- aBasUSDC (aave)
     */

) as temp_table (contract_address)
