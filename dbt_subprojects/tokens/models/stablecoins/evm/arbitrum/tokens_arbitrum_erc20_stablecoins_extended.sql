{% set chain = 'arbitrum' %}

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
-- add new stablecoins here (not in tokens_arbitrum_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address
from (values

     (0x4933a85b5b5466fbaf179f72d3de273c287ec2c2), -- EURAU
     (0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553)  -- ZCHF

     /* rebasing / interest accruing tokens
     (0x7cfadfd5645b50be87d546f42699d863648251ad), -- stataArbUSDCn (static aave)
     (0xb165a74407fe1e519d6bcbdec1ed3202b35a4140), -- stataArbUSDT (static aave)
     (0x724dc807b04555b71ed48a6896b6f41593b8c637), -- aArbUSDCn (aave)
     (0x0b2b2b2076d95dda7817e785989fe353fe955ef9), -- sUSDai (staked)
     (0x3509f19581afedeff07c53592bc0ca84e4855475)  -- xUSD (synthetic)
     */

) as temp_table (contract_address)
