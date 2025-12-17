{% set chain = 'blast' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_blast_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x4300000000000000000000000000000000000003), -- USDB
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34), -- USDe
     (0xeb466342c4d449bc9f53a865d5cb90586f405215), -- axlUSDC
     (0x211cc4dd073734da055fbf44a2b4667d5e5fe5d2), -- sUSDe
     (0x52056ed29fe015f4ba2e3b079d10c0b87f46e8c6), -- USDz
     (0x9d889e2d7ff49ece580e5354ce934ccd1d6e78dc), -- expUSD
     (0x1a3d9b2fa5c6522c8c071dc07125ce55df90b253), -- DUSD
     (0xc608dfb90a430df79a8a1edbc8be7f1a0eb4e763), -- arUSD
     (0x836aed3b0e0ee44c77e0b6db34d170abcce9baac), -- USDBx
     (0x578122317baca7a3c7bb5301460d2f4f96e9394a), -- DUSD
     (0x837fe561e9c5dfa73f607fda679295dbc2be5e40)  -- MUSD

) as temp_table (contract_address)
