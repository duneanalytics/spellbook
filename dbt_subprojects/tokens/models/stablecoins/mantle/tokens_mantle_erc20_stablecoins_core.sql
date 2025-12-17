{% set chain = 'mantle' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_mantle_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, backing, symbol, decimals, name
from (values

     (0x201eba5cc46d216ce6dc03f6a759e8e766e956ae, 'Fiat-backed stablecoin', 'USDT', 6, ''),
     (0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9, 'Fiat-backed stablecoin', 'USDC', 6, ''),
     (0x5be26527e817998a7206475496fde1e68957c5a6, 'Fiat-backed stablecoin', 'USDY', 18, ''),
     (0xeb466342c4d449bc9f53a865d5cb90586f405215, 'Crypto-backed stablecoin', 'axlUSDC', 6, ''),
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'Crypto-backed stablecoin', 'USDe', 18, '')

) as temp_table (contract_address, backing, symbol, decimals, name)
