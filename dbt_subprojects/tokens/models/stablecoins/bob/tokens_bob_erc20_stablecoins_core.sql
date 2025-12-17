{% set chain = 'bob' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_bob_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, backing, symbol, decimals, name
from (values

     (0x6c851f501a3f24e29a8e39a29591cddf09369080, 'Crypto-backed stablecoin', 'DAI', 18, 'Dai Stablecoin'),
     (0xf3107eec1e6f067552c035fd87199e1a5169cb20, 'Crypto-backed stablecoin', 'DLLR', 18, 'Sovryn DLLR'),
     (0xc4a20a608616f18aa631316eeda9fb62d089361e, 'Hybrid stablecoin', 'FRAX', 18, 'FRAX'),
     (0xe75d0fb2c24a55ca1e3f96781a2bcc7bdba058f0, 'Fiat-backed stablecoin', 'USDC', 6, 'USD Coin'),
     (0x05d032ac25d322df992303dca074ee7392c117b9, 'Fiat-backed stablecoin', 'USDT', 6, 'Tether USD')

) as temp_table (contract_address, backing, symbol, decimals, name)
