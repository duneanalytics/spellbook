{% set chain = 'arbitrum' %}

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
-- new stablecoins should be added to tokens_arbitrum_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x641441c631e2f909700d2f41fd87f0aa6a6b4edb, 'USD'), -- USX
     (0x680447595e8b7b3aa1b43beb9f6098c79ac2ab3f, 'USD'), -- USDD
     (0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9, 'USD'), -- USDT
     (0xaf88d065e77c8cc2239327c5edb3a432268e5831, 'USD'), -- USDC
     (0xfea7a6a0b346362bf88a9e4a88416b77a57d6c2a, 'USD'), -- MIM
     (0xa970af1a584579b618be4d69ad6f73459d112f95, 'USD'), -- sUSD
     (0xe80772eaf6e2e18b651f160bc9158b2a5cafca65, 'USD'), -- USD+
     (0x17fc002b466eec40dae837fc4be5c67993ddbd6f, 'USD'), -- FRAX
     (0xda10009cbd5d07dd0cecc66161fc93d7c9000da1, 'USD'), -- DAI
     (0x64343594ab9b56e99087bfa6f2335db24c2d1f17, 'USD'), -- VST
     (0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'USD'), -- MAI
     (0xff970a61a04b1ca14834a43f5de4533ebddb5cc8, 'USD'), -- USDC.e
     (0x4d15a3a2286d883af0aa1b3f21367843fac63e07, 'USD'), -- TUSD
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'USD'), -- USDe
     (0xeb466342c4d449bc9f53a865d5cb90586f405215, 'USD'), -- axlUDC
     (0x12275dcb9048680c4be40942ea4d92c74c63b844, 'USD'), -- eUSD
     (0x323665443cef804a3b5206103304bd4872ea4253, 'USD'), -- USDV
     (0x4cfa50b7ce747e2d61724fcac57f24b748ff2b2a, 'USD'), -- fUSDC
     (0x7dff72693f6a4149b17e7c6314655f6a9f7c8b33, 'USD'), -- GHO
     (0xf3527ef8de265eaa3716fb312c12847bfba66cef, 'USD'), -- USDX
     (0x4bdc50829003cc017443bf9bfb3ac82f3f0c4ad4, 'CLP'), -- CLPC
     (0x5e85faf503621830ca857a5f38b982e0cc57d537, 'EUR'), -- dEURO
     (0xfa5ed56a203466cbbc2430a43c66b9d8723528e7, 'EUR'), -- agEUR
     (0x0c06ccf38114ddfc35e07427b9424adcca9f44f8, 'EUR'), -- EURe
     (0xe333e7754a2dc1e020a162ecab019254b9dab653, 'SGD'), -- XSGD
     (0xf197ffc28c23e0309b5559e7a166f2c6164c80aa, 'MXN'), -- MXNB
     (0x4883c8f0529f37e40ebea870f3c13cdfad5d01f8, 'EUR'), -- VEUR
     (0x2b28e826b55e399f4d4699b85f68666ac51e6f70, 'CAD'), -- CADC
     (0x589d35656641d6ab57a545f08cf473ecd9b6d5f7, 'JPY'), -- GYEN
     (0x46850ad61c2b7d64d08c9c754f45254596696984, 'USD'), -- PYUSD
     (0x6491c05a82219b8d1479057361ff1654749b876b, 'USD'), -- USDS

     (0x0a1a1a107e45b7ced86833863f482bc5f4ed82ef, 'USD'), -- USDai
     (0xbe00f3db78688d9704bcb4e0a827aea3a9cc0d62, 'USD')  -- USD24

) as temp_table (contract_address, currency)
