{% set chain = 'worldchain' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_worldchain_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x18bc5bcc660cf2b9ce3cd51a404afe1a0cbd3c22), -- IDRX
     (0x79a02482a880bce3f13e09da970dc34db4cd24d1), -- USDC.e
     (0x0dc4f92879b7670e5f4e4e6e3c801d229129d90d), -- wARS
     (0x337e7456b420bd3481e7fa61fa9850343d610d34), -- wMXN
     (0xd76f5faf6888e24d9f04bf92a0c8b921fe4390e0), -- wBRL
     (0x2c537e5624e4af88a7ae4060c022609376c8d0eb)  -- TRYB

) as temp_table (contract_address)
