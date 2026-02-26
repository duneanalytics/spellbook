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

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x4933a85b5b5466fbaf179f72d3de273c287ec2c2, 'EUR'), -- EURAU
     (0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553, 'CHF')  -- ZCHF

     /* yield-bearing / rebasing tokens
     (0xddc0385169797937066bbd8ef409b5b3c0dfeb52, 'USD'), -- wUSDR
     (0xd74f5255d557944cf7dd0e45ff521520002d5748, 'USD'), -- USDs
     (0xb1084db8d3c05cebd5fa9335df95ee4b8a0edc30, 'USD'), -- USDT+
     (0x59d9356e565ab3a36dd77763fc0d87feaf85508c, 'USD'), -- USDM
     (0x57f5e098cad7a3d1eed53991d4d66c45c9af7812, 'USD'), -- sUSDM (list: wUSDM)
     (0xd3443ee1e91af28e5fb858fbd0d72a63ba8046e0, 'USD'), -- gUSDC
     */

     /* rebasing / interest accruing tokens
     (0x7cfadfd5645b50be87d546f42699d863648251ad), -- stataArbUSDCn (static aave)
     (0xb165a74407fe1e519d6bcbdec1ed3202b35a4140), -- stataArbUSDT (static aave)
     (0x724dc807b04555b71ed48a6896b6f41593b8c637), -- aArbUSDCn (aave)
     (0x0b2b2b2076d95dda7817e785989fe353fe955ef9), -- sUSDai (staked)
     (0x3509f19581afedeff07c53592bc0ca84e4855475)  -- xUSD (synthetic)
     */

) as temp_table (contract_address, currency)
