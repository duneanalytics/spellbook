{% set chain = 'worldchain' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

select '{{chain}}' as blockchain, contract_address, backing, symbol, decimals, name
from (values

     (0x18bc5bcc660cf2b9ce3cd51a404afe1a0cbd3c22, 'Fiat-backed stablecoin', 'IDRX', 18, '')

) as temp_table (contract_address, backing, symbol, decimals, name)
