{% set chain = 'celo' %}

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
-- new stablecoins should be added to tokens_celo_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0xfecb3f7c54e2caae9dc6ac9060a822d47e053760), -- BRLA
     (0x8a567e2ae79ca692bd748ab832081c45de4041ea), -- cCOP
     (0xfaea5f3404bba20d3cc2f8c4b0a888f55a3c7313), -- cGHS
     (0x456a3d042c0dbd3db53d5489e98dfb038553b0d0), -- cKES
     (0xe2702bd97ee33c88c8f6f92da3b733608aa76f71), -- cNGN
     (0xc92e8fc2947e32f2b574cca9f2f12097a71d5606), -- COPM
     (0xe8537a3d056da446677b9e9d6c5db704eaab4787), -- cREAL
     (0x4c35853a3b4e647fd266f4de678dcc8fec410bf6), -- cZAR
     (0xc16b81af351ba9e64c1a069e3ab18c244a1e3049), -- agEUR
     (0x73f93dcc49cb8a239e2032663e9475dd5ef29a08), -- eXOF
     (0x105d4a9306d2e55a71d2eb95b81553ae1dc20d7b), -- PUSO
     (0x9346f43c1588b6df1d52bdd6bf846064f92d9cba), -- VEUR
     (0x7ae4265ecfc1f31bc0e112dfcfe3d78e01f4bb7f), -- VGBP

     (0x765de816845861e75a25fca122bb6898b8b1282a), -- cUSD
     (0xceba9300f2b948710d2653dd7b07f33a8b32118c), -- USDC
     (0x48065fbbe25f71c9282ddf5e1cd6d6a887483d5e), -- USD₮
     (0xeb466342c4d449bc9f53a865d5cb90586f405215), -- axlUSDC
     (0x4f604735c1cf31399c6e711d5962b2b3e0225ad3)  -- USDGLO

) as temp_table (contract_address)
