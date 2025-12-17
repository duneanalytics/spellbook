{% set chain = 'gnosis' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_seed',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- seed list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_gnosis_erc20_stablecoins_latest

select '{{chain}}' as blockchain, contract_address, backing, symbol, decimals, name
from (values

     (0xdd96b45877d0e8361a4ddb732da741e97f3191ff, 'Fiat-backed stablecoin', 'BUSD', 18, ''),
     (0x4ecaba5870353805a9f068101a40e0f32ed605c6, 'Fiat-backed stablecoin', 'USDT', 6, ''),
     (0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'Crypto-backed stablecoin', 'MAI', 18, ''),
     (0x44fa8e6f47987339850636f88629646662444217, 'Hybrid stablecoin', 'DAI', 18, ''),
     (0xddafbb505ad214d7b80b1f830fccc89b60fb7a83, 'Fiat-backed stablecoin', 'USDC', 6, ''),
     (0xfecb3f7c54e2caae9dc6ac9060a822d47e053760, 'Fiat-backed stablecoin', 'BRLA', 18, ''),
     (0x4b1e2c2762667331bc91648052f646d1b0d35984, 'Crypto-backed stablecoin', 'agEUR', 18, ''),
     (0x420ca0f9b9b604ce0fd9c18ef134c705e5fa3430, 'Fiat-backed stablecoin', 'EURe', 18, ''),
     (0x2a22f9c3b484c3629090feed35f17ff8f88f76f0, 'Fiat-backed stablecoin', 'USDC', 6, 'Circle')

) as temp_table (contract_address, backing, symbol, decimals, name)
