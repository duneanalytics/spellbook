{% set chain = 'bob' %}

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
-- new stablecoins should be added to tokens_bob_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x6c851f501a3f24e29a8e39a29591cddf09369080), -- DAI
     (0xf3107eec1e6f067552c035fd87199e1a5169cb20), -- DLLR
     (0xc4a20a608616f18aa631316eeda9fb62d089361e), -- FRAX
     (0xe75d0fb2c24a55ca1e3f96781a2bcc7bdba058f0), -- USDC
     (0x05d032ac25d322df992303dca074ee7392c117b9)  -- USDT

) as temp_table (contract_address)
