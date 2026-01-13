{% set chain = 'xlayer' %}

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
-- new stablecoins should be added to tokens_xlayer_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x1e4a5963abfd975d8c9021ce480b42188849d41d), -- USDT
     (0x779ded0c9e1022225f8e0630b35a9b54be713736), -- USD₮0
     (0x4ae46a509f6b1d9056937ba4500cb143933d2dc8)  -- USDG

) as temp_table (contract_address)
