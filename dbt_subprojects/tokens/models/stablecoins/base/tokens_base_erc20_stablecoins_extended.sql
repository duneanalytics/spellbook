{% set chain = 'base' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_extended',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- extended list: new stablecoin addresses added after the core list was frozen
-- add new stablecoins here (not in tokens_base_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, backing, symbol, decimals, name
from (values

     (0x0000000000000000000000000000000000000000, '', '', 0, '')

) as temp_table (contract_address, backing, symbol, decimals, name)
where contract_address != 0x0000000000000000000000000000000000000000
