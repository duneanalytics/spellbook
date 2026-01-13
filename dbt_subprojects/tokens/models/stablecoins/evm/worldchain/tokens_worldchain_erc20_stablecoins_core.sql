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

     (0x79a02482a880bce3f13e09da970dc34db4cd24d1)  -- USDC.e

) as temp_table (contract_address)
