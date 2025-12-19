{% set chain = 'fantom' %}

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
-- new stablecoins should be added to tokens_fantom_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x5f0456f728e2d59028b4f5b8ad8c604100724c6a), -- L3USD
     (0x3129662808bec728a27ab6a6b9afd3cbaca8a43c), -- DOLA
     (0x9879abdea01a879644185341f7af7d8343556b7a), -- TUSD
     (0x0def844ed26409c5c46dda124ec28fb064d90d27), -- CoUSD
     (0xb9d62c829fbf7eaff1eba4e50f3d0480b66c1748), -- PDO
     (0x7a6e4e3cc2ac9924605dca4ba31d1831c84b44ae), -- 2OMB
     (0x87a5c9b60a3aaf1064006fe64285018e50e0d020), -- MAGIK
     (0x74e23df9110aa9ea0b6ff2faee01e740ca1c642e), -- TOR
     (0xfb98b335551a418cd0737375a2ea0ded62ea213b), -- miMATIC
     (0x04068da6c83afcfa0e13ba15a6696662335d5b75), -- USDC
     (0x82f0b8b456c1a451378467398982d4834b6829c1), -- MIM
     (0xdc301622e621166bd8e82f2ca0a26c13ad0be355), -- FRAX
     (0xb67fa6defce4042070eb1ae1511dcd6dcc6a532e), -- alUSD
     (0x846e4d51d7e2043c1a87e0ab7490b93fb940357b), -- UST
     (0xad84341756bf337f5a0164515b1f6f993d194e1f), -- FUSD
     (0x049d68029688eabf473097a2fc38ef61633a3c7a), -- fUSDT
     (0xe2d27f06f63d98b8e11b38b5b08a75d0c8dd62b9), -- UST
     (0x8d11ec38a3eb5e956b052f67da8bdc9bef8abf3e), -- DAI
     (0x6fc9383486c163fa48becdec79d6058f984f62ca), -- USDB
     (0xc54a1684fd1bef1f077a336e6be4bd9a3096a6ca), -- 2SHARES
     (0x1d3918043d22de2d799a4d80f72efd50db90b5af), -- sPDO

     (0x1b6382dbdea11d97f24495c9a90b7c88469134a4)  -- axlUSDC

) as temp_table (contract_address)
