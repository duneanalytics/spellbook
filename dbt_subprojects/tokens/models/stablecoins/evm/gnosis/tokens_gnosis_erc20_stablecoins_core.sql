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

select '{{chain}}' as blockchain, contract_address
from (values

     (0xdd96b45877d0e8361a4ddb732da741e97f3191ff), -- BUSD
     (0x4ecaba5870353805a9f068101a40e0f32ed605c6), -- USDT
     (0x3f56e0c36d275367b8c502090edf38289b3dea0d), -- MAI
     (0x44fa8e6f47987339850636f88629646662444217), -- DAI
     (0xddafbb505ad214d7b80b1f830fccc89b60fb7a83), -- USDC
     (0xfecb3f7c54e2caae9dc6ac9060a822d47e053760), -- BRLA
     (0x4b1e2c2762667331bc91648052f646d1b0d35984), -- agEUR
     (0x420ca0f9b9b604ce0fd9c18ef134c705e5fa3430), -- EURe
     (0x2a22f9c3b484c3629090feed35f17ff8f88f76f0)  -- USDC

) as temp_table (contract_address)
