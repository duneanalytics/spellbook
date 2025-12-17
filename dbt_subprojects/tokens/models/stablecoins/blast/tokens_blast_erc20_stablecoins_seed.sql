{% set chain = 'blast' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_seed',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- seed list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_blast_erc20_stablecoins_latest

select '{{chain}}' as blockchain, contract_address, backing, symbol, decimals, name
from (values

     (0x4300000000000000000000000000000000000003, 'Hybrid stablecoin', 'USDB', 18, ''),
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'Crypto-backed stablecoin', 'USDe', 18, ''),
     (0xeb466342c4d449bc9f53a865d5cb90586f405215, 'Crypto-backed stablecoin', 'axlUSDC', 6, ''),
     (0x211cc4dd073734da055fbf44a2b4667d5e5fe5d2, 'Crypto-backed stablecoin', 'sUSDe', 18, ''),
     (0x52056ed29fe015f4ba2e3b079d10c0b87f46e8c6, 'Fiat-backed stablecoin', 'USDz', 18, ''),
     (0x9d889e2d7ff49ece580e5354ce934ccd1d6e78dc, 'Crypto-backed stablecoin', 'expUSD', 18, ''),
     (0x1a3d9b2fa5c6522c8c071dc07125ce55df90b253, 'Fiat-backed stablecoin', 'DUSD', 18, ''),
     (0xc608dfb90a430df79a8a1edbc8be7f1a0eb4e763, 'Crypto-backed stablecoin', 'arUSD', 18, ''),
     (0x836aed3b0e0ee44c77e0b6db34d170abcce9baac, 'Fiat-backed stablecoin', 'USDBx', 18, ''),
     (0x578122317baca7a3c7bb5301460d2f4f96e9394a, 'Crypto-backed stablecoinn', 'DUSD', 18, ''),
     (0x837fe561e9c5dfa73f607fda679295dbc2be5e40, 'Crypto-backed stablecoin', 'MUSD', 18, '')

) as temp_table (contract_address, backing, symbol, decimals, name)
