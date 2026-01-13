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

select '{{chain}}' as blockchain, contract_address
from (values

     (0x641441c631e2f909700d2f41fd87f0aa6a6b4edb), -- USX
     (0x680447595e8b7b3aa1b43beb9f6098c79ac2ab3f), -- USDD
     (0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9), -- USDT
     (0xaf88d065e77c8cc2239327c5edb3a432268e5831), -- USDC
     (0xfea7a6a0b346362bf88a9e4a88416b77a57d6c2a), -- MIM
     (0xa970af1a584579b618be4d69ad6f73459d112f95), -- sUSD
     (0xddc0385169797937066bbd8ef409b5b3c0dfeb52), -- wUSDR
     (0xe80772eaf6e2e18b651f160bc9158b2a5cafca65), -- USD+
     (0x17fc002b466eec40dae837fc4be5c67993ddbd6f), -- FRAX
     (0xda10009cbd5d07dd0cecc66161fc93d7c9000da1), -- DAI
     (0x64343594ab9b56e99087bfa6f2335db24c2d1f17), -- VST
     (0xd74f5255d557944cf7dd0e45ff521520002d5748), -- USDs
     (0x3f56e0c36d275367b8c502090edf38289b3dea0d), -- MAI
     (0xb1084db8d3c05cebd5fa9335df95ee4b8a0edc30), -- USDT+
     (0x3509f19581afedeff07c53592bc0ca84e4855475), -- xUSD
     (0x59d9356e565ab3a36dd77763fc0d87feaf85508c), -- USDM
     (0xff970a61a04b1ca14834a43f5de4533ebddb5cc8), -- USDC.e
     (0x4d15a3a2286d883af0aa1b3f21367843fac63e07), -- TUSD
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34), -- USDe
     (0xeb466342c4d449bc9f53a865d5cb90586f405215), -- axlUDC
     (0xd3443ee1e91af28e5fb858fbd0d72a63ba8046e0), -- gUSDC
     (0x7cfadfd5645b50be87d546f42699d863648251ad), -- stataArbUSDCn
     (0x12275dcb9048680c4be40942ea4d92c74c63b844), -- eUSD
     (0xb165a74407fe1e519d6bcbdec1ed3202b35a4140), -- stataArbUSDT
     (0x323665443cef804a3b5206103304bd4872ea4253), -- USDV
     (0x4cfa50b7ce747e2d61724fcac57f24b748ff2b2a), -- fUSDC
     (0x57f5e098cad7a3d1eed53991d4d66c45c9af7812), -- sUSDM
     (0x7dff72693f6a4149b17e7c6314655f6a9f7c8b33), -- GHO
     (0xf3527ef8de265eaa3716fb312c12847bfba66cef), -- USDX
     (0x4bdc50829003cc017443bf9bfb3ac82f3f0c4ad4), -- CLPC
     (0x5e85faf503621830ca857a5f38b982e0cc57d537), -- dEURO
     (0xfa5ed56a203466cbbc2430a43c66b9d8723528e7), -- agEUR
     (0x0c06ccf38114ddfc35e07427b9424adcca9f44f8), -- EURe
     (0xe333e7754a2dc1e020a162ecab019254b9dab653), -- XSGD
     (0xf197ffc28c23e0309b5559e7a166f2c6164c80aa), -- MXNB
     (0x4883c8f0529f37e40ebea870f3c13cdfad5d01f8), -- VEUR
     (0x2b28e826b55e399f4d4699b85f68666ac51e6f70), -- CADC
     (0x589d35656641d6ab57a545f08cf473ecd9b6d5f7), -- GYEN
     (0x46850ad61c2b7d64d08c9c754f45254596696984), -- PYUSD
     (0x6491c05a82219b8d1479057361ff1654749b876b), -- USDS

     (0x0a1a1a107e45b7ced86833863f482bc5f4ed82ef), -- USDai
     (0xbe00f3db78688d9704bcb4e0a827aea3a9cc0d62), -- USD24
     (0x724dc807b04555b71ed48a6896b6f41593b8c637), -- aArbUSDCn
     (0x0b2b2b2076d95dda7817e785989fe353fe955ef9)  -- sUSDai

) as temp_table (contract_address)
