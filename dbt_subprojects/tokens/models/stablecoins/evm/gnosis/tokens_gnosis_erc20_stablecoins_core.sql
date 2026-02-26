{% set chain = 'gnosis' %}

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
-- new stablecoins should be added to tokens_gnosis_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0xdd96b45877d0e8361a4ddb732da741e97f3191ff, 'USD'), -- BUSD
     (0x4ecaba5870353805a9f068101a40e0f32ed605c6, 'USD'), -- USDT
     (0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'USD'), -- MAI
     (0x44fa8e6f47987339850636f88629646662444217, 'USD'), -- DAI
     (0xddafbb505ad214d7b80b1f830fccc89b60fb7a83, 'USD'), -- USDC
     (0xfecb3f7c54e2caae9dc6ac9060a822d47e053760, 'BRL'), -- BRLA
     (0x4b1e2c2762667331bc91648052f646d1b0d35984, 'EUR'), -- agEUR
     (0x420ca0f9b9b604ce0fd9c18ef134c705e5fa3430, 'EUR'), -- EURe
     (0x2a22f9c3b484c3629090feed35f17ff8f88f76f0, 'USD'), -- USDC
     (0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553, 'CHF')  -- ZCHF

) as temp_table (contract_address, currency)
