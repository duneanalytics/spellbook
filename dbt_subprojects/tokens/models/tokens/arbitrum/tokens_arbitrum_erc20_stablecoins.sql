{{ config(
      schema = 'tokens_arbitrum'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["arbitrum"]\',
                                  "sector",
                                  "tokens_arbitrum",
                                  \'["synthquest","rantum"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES

        ('arbitrum', 0x641441c631e2f909700d2f41fd87f0aa6a6b4edb, 'Crypto-backed stablecoin', 'USX', 18, ''),
        ('arbitrum', 0x680447595e8b7b3aa1b43beb9f6098c79ac2ab3f, 'Algorithmic stablecoin', 'USDD', 18, ''),
        ('arbitrum', 0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9, 'Fiat-backed stablecoin', 'USDT', 6, ''),
        ('arbitrum', 0xaf88d065e77c8cc2239327c5edb3a432268e5831, 'Fiat-backed stablecoin', 'USDC', 6, ''),
        ('arbitrum', 0xfea7a6a0b346362bf88a9e4a88416b77a57d6c2a, 'Crypto-backed stablecoin', 'MIM', 18, ''),
        ('arbitrum', 0xa970af1a584579b618be4d69ad6f73459d112f95, 'Crypto-backed stablecoin', 'sUSD', 18, ''),
        ('arbitrum', 0xddc0385169797937066bbd8ef409b5b3c0dfeb52, 'RWA-backed stablecoin', 'wUSDR', 9, ''),
        ('arbitrum', 0xe80772eaf6e2e18b651f160bc9158b2a5cafca65, 'Crypto-backed stablecoin', 'USD+', 6, ''),
        ('arbitrum', 0x17fc002b466eec40dae837fc4be5c67993ddbd6f, 'Hybrid stablecoin', 'FRAX', 18, ''),
        ('arbitrum', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1, 'Hybrid stablecoin', 'DAI', 18, ''),
        ('arbitrum', 0x64343594ab9b56e99087bfa6f2335db24c2d1f17, 'Crypto-backed stablecoin', 'VST', 18, ''),
        ('arbitrum', 0xd74f5255d557944cf7dd0e45ff521520002d5748, 'Crypto-backed stablecoin', 'USDs', 18, ''),
        ('arbitrum', 0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'Crypto-backed stablecoin', 'MAI', 18, ''),
        ('arbitrum', 0xb1084db8d3c05cebd5fa9335df95ee4b8a0edc30, 'Crypto-backed stablecoin', 'USDT+', 6, ''),
        ('arbitrum', 0x3509f19581afedeff07c53592bc0ca84e4855475, 'Crypto-backed stablecoin', 'xUSD', 18, ''),
        ('arbitrum', 0x59d9356e565ab3a36dd77763fc0d87feaf85508c, 'Fiat-backed stablecoin', 'USDM', 18, ''),
        ('arbitrum', 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8, 'Fiat-backed stablecoin', 'USDC', 6, ''),
        ('arbitrum', 0x4d15a3a2286d883af0aa1b3f21367843fac63e07, 'Fiat-backed stablecoin', 'TUSD', 18, ''),
        ('arbitrum', 0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'Crypto-backed stablecoin', 'USDe', 18, ''),
        ('arbitrum', 0xeb466342c4d449bc9f53a865d5cb90586f405215, 'Crypto-backed stablecoin', 'axlUDC', 6, ''),
        ('arbitrum', 0xd3443ee1e91af28e5fb858fbd0d72a63ba8046e0, 'Crypto-backed stablecoin', 'gUSDC', 6, ''),
        ('arbitrum', 0x7cfadfd5645b50be87d546f42699d863648251ad, 'Crypto-backed stablecoin', 'stataArbUSDCn', 6, ''),
        ('arbitrum', 0x12275dcb9048680c4be40942ea4d92c74c63b844, 'Crypto-backed stablecoin', 'eUSD', 18, ''),
        ('arbitrum', 0xb165a74407fe1e519d6bcbdec1ed3202b35a4140, 'Crypto-backed stablecoin', 'stataArbUSDT', 6, ''),
        ('arbitrum', 0x323665443cef804a3b5206103304bd4872ea4253, 'Crypto-backed stablecoin', 'USDV', 6, ''),
        ('arbitrum', 0x4cfa50b7ce747e2d61724fcac57f24b748ff2b2a, 'Crypto-backed stablecoin', 'fUSDC', 6, ''),
        ('arbitrum', 0x57f5e098cad7a3d1eed53991d4d66c45c9af7812, 'Crypto-backed stablecoin', 'sUSDM', 18, ''),
        ('arbitrum', 0x7dff72693f6a4149b17e7c6314655f6a9f7c8b33, 'Crypto-backed stablecoin', 'GHO', 18, '')

     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
