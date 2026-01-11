{% set chain = 'berachain' %}

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
-- new stablecoins should be added to tokens_berachain_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x549943e04f40284185054145c6e4e9568c1d3241), -- USDC.e
     (0x779ded0c9e1022225f8e0630b35a9b54be713736), -- USD₮0
     (0x688e72142674041f8f6af4c808a4045ca1d6ac82), -- BYUSD
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34)  -- USDe

) as temp_table (contract_address)
