{% set chain = 'scroll' %}

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
-- new stablecoins should be added to tokens_scroll_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0xf55bec9cafdbe8730f096aa55dad6d22d44099df), -- USDT
     (0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4), -- USDC
     (0xca77eb3fefe3725dc33bccb54edefc3d9f764f97), -- DAI
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34), -- USDe
     (0x77fbf86399ed764a084f77b9accb049f3dbc32d2), -- loreUSD
     (0xedeabc3a1e7d21fe835ffa6f83a710c70bb1a051)  -- LUSD

) as temp_table (contract_address)
