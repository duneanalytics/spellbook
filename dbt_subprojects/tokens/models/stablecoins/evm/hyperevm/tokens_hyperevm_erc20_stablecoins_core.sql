{% set chain = 'hyperevm' %}

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
-- new stablecoins should be added to tokens_hyperevm_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb), -- USD₮0
     (0xb88339cb7199b77e23db6e890353e22632ba630f), -- USDC
     (0x111111a1a0667d36bd57c0a9f569b98057111111), -- USDH
     (0xb50a96253abdf803d85efcdce07ad8becbc52bd5), -- USDHL
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34), -- USDe
     (0x02c6a2fa58cc01a18b8d9e00ea48d65e4df26c70), -- feUSD
     (0xca79db4b49f608ef54a5cb813fbed3a6387bc645), -- USDXL
     (0x5e105266db42f78fa814322bce7f388b4c2e61eb)  -- hbUSDT

) as temp_table (contract_address)
