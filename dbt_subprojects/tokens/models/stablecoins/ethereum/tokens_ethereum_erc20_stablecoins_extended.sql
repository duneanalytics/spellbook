{% set chain = 'ethereum' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_extended',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- extended list: new stablecoin addresses added after the core list was frozen
-- add new stablecoins here (not in tokens_ethereum_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, backing, symbol, decimals, name, denomination
from (values

     (0x0000000000000000000000000000000000000000, '', '', 0, '', '')

) as temp_table (contract_address, backing, symbol, decimals, name, denomination)
where contract_address != 0x0000000000000000000000000000000000000000
