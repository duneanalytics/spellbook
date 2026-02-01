{% set chain = 'bnb' %}

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
-- add new stablecoins here (not in tokens_bnb_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address
from (values

     (0xc1fdbed7dac39cae2ccc0748f7a80dc446f6a594)  -- TRYB

     /* rebasing / interest accruing tokens
     (0x8ba9da757d1d66c58b1ae7e2ed6c04087348a82d), -- sUSDD (staked USDD)
     (0x64748ea3e31d0b7916f0ff91b017b9f404ded8ef), -- cUSDO (lending)
     (0x6458df5d764284346c19d88a104fd3d692471499)  -- iUSDS (iron bank)
     */

) as temp_table (contract_address)
