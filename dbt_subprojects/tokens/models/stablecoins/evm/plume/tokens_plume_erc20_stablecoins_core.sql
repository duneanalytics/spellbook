{% set chain = 'plume' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental transfers
-- new stablecoins should be added to tokens_plume_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0xdddd73f5df1f0dc31373357beac77545dc5a6f3f, 'USD'), -- pUSD
     (0x78add880a697070c1e765ac44d65323a0dcce913, 'USD'), -- USDC.e
     (0xda6087e69c51e7d31b6dbad276a3c44703dfdcad, 'USD'), -- USDT
     (0x222365ef19f7947e5484218551b56bb3965aa7af, 'USD')  -- USDC

) as temp_table (contract_address, currency)
