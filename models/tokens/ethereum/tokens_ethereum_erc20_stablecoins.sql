{{ config(
      alias = 'stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "sector",
                                  "tokens_ethereum",
                                  \'["gentrexha", "dot2dotseurat"]\') }}'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (VALUES
          (0xbc6da0fe9ad5f3b0d58160288917aa56653660e9, 'alUSD',	18,	'Alchemix USD'),
          (0xd46ba6d942050d489dbd938a2c909a5d5039a161, 'AMPL',	9,	'Ampleforth'),
          (0x4fabb145d64652a948d72533023f6e7a623c7c53, 'BUSD',	18,	'Binance USD'),
          (0x6b175474e89094c44da98b954eedeac495271d0f, 'DAI',	18,	'Dai'),
          (0xdb25f211ab05b1c97d595516f45794528a807ad8, 'EURS',	18,	'STASIS EURS'),
          (0x956f47f50a910163d8bf957cf5846d573e7f87ca, 'FEI',	18,	'Fei USD'),
          (0x853d955acef822db058eb8505911ed77f175b99e, 'FRAX',	18,	'Frax'),
          (0x056fd409e1d7a124bd7017459dfea2f387b6d5cd, 'GUSD',	2,	'Gemini Dollar'),
          (0xdf574c24545e5ffecb9a659c229253d4111d87e1, 'HUSD',	8,	'HUSD'),
          (0x5f98805A4E8be255a32880FDeC7F6728C6568bA0, 'LUSD',	18,	'Liquity USD'),
          (0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3, 'MIM',	18,	'Magic Internet Money'),
          (0xe2f2a5c287993345a840db3b0845fbc70f5935a5, 'MUSD',	18,	'mStable USD'),
          (0x8e870d67f660d95d5be530380d0ec0bd388289e1, 'PAX',	18,	'Paxos Standard'),
          (0x03ab458634910aad20ef5f1c8ee96f1d6ac54919, 'RAI',	18,	'Rai Reflex Index'),
          (0x57Ab1ec28D129707052df4dF418D58a2D46d5f51, 'sUSD',	18,	'Synthetix sUSD'),
          (0x57Ab1E02fEE23774580C119740129eAC7081e9D3, 'sUSD',	18,	'Synthetix sUSD'),
          (0x0000000000085d4780b73119b644ae5ecd22b376, 'TUSD',	18,	'TrueUSD'),
          (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 'USDC',	6,	'USD Coin'),
          (0x674c6ad92fd080e4004b2312b45f796a192d27a0, 'USDN',	18,	'Neutrino USD'),
          (0x1456688345527bE1f37E9e627DA0837D6f08C925, 'USDP',	18,	'Pax Dollar'),
          (0xdac17f958d2ee523a2206206994597c13d831ec7, 'USDT',	6,	'Tether'),
          (0xa47c8bf37f92abed4a126bda807a7b7498661acd, 'UST',	18,	'Wrapped UST Token'),
          (0xa693b19d2931d498c5b318df961919bb4aee87a5, 'UST',	6,	'UST (Wormhole)'),
          (0xe07F9D810a48ab5c3c914BA3cA53AF14E4491e8A, 'GYD', 18, 'Gyro Dollar')
     ) AS temp_table (contract_address, symbol, decimals, name)
