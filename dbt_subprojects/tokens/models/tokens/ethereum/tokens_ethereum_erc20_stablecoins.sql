{{ config(
      schema = 'tokens_ethereum'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "sector",
                                  "tokens_ethereum",
                                  \'["Henrystats", "synthquest"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES

    ('ethereum', 0x6b175474e89094c44da98b954eedeac495271d0f, 'Hybrid stablecoin', 'DAI', 18, 'Dai'),
    ('ethereum', 0x96f6ef951840721adbf46ac996b59e0235cb985c, 'Fiat-backed stablecoin', 'USDY', 18, ''),
    ('ethereum', 0xc5f0f7b66764f6ec8c8dff7ba683102295e16409, 'Fiat-backed stablecoin', 'FDUSD', 18, ''),
    ('ethereum', 0x8d6cebd76f18e1558d4db88138e2defb3909fad6, 'Crypto-backed stablecoin', 'MAI', 18, ''),
    ('ethereum', 0x5f98805a4e8be255a32880fdec7f6728c6568ba0, 'Crypto-backed stablecoin', 'LUSD', 18, 'Liquity USD'),
    ('ethereum', 0xf9c2b386ff5df088ac717ab0010587bad3bc1ab1, 'Hybrid stablecoin', 'iUSDS', 18, ''),
    ('ethereum', 0x45fdb1b92a649fb6a64ef1511d3ba5bf60044838, 'Hybrid stablecoin', 'USDS', 18, ''),
    ('ethereum', 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 'Fiat-backed stablecoin', 'USDC', 6, 'USD Coin'),
    ('ethereum', 0xdac17f958d2ee523a2206206994597c13d831ec7, 'Fiat-backed stablecoin', 'USDT', 6, 'Tether'),
    ('ethereum', 0xbea0000029ad1c77d3d5d23ba2d8893db9d1efab, 'Algorithmic stablecoin', 'BEAN', 6, ''),
    ('ethereum', 0xe2f2a5c287993345a840db3b0845fbc70f5935a5, 'Crypto-backed stablecoin', 'mUSD', 18, 'mStable USD'),
    ('ethereum', 0x7712c34205737192402172409a8f7ccef8aa2aec, 'Fiat-backed stablecoin', 'BUIDL', 6, ''),
    ('ethereum', 0x4fabb145d64652a948d72533023f6e7a623c7c53, 'Fiat-backed stablecoin', 'BUSD', 18, 'Binance USD'),
    ('ethereum', 0x1456688345527be1f37e9e627da0837d6f08c925, 'Crypto-backed stablecoin', 'USDP', 18, 'Pax Dollar'),
    ('ethereum', 0x2a8e1e676ec238d8a992307b495b45b3feaa5e86, 'Crypto-backed stablecoin', 'OUSD', 18, ''),
    ('ethereum', 0x0c10bf8fcb7bf5412187a595ab97a3609160b5c6, 'Algorithmic stablecoin', 'USDD', 18, ''),
    ('ethereum', 0x8e870d67f660d95d5be530380d0ec0bd388289e1, 'Fiat-backed stablecoin', 'USDP', 18, 'Paxos Standard'),
    ('ethereum', 0x853d955acef822db058eb8505911ed77f175b99e, 'Hybrid stablecoin', 'FRAX', 18, 'Frax'),
    ('ethereum', 0x6c3ea9036406852006290770bedfcaba0e23a0e8, 'Fiat-backed stablecoin', 'PYUSD', 6, ''),
    ('ethereum', 0xdf574c24545e5ffecb9a659c229253d4111d87e1, 'Fiat-backed stablecoin', 'HUSD', 8, 'HUSD'),
    ('ethereum', 0xc285b7e09a4584d027e5bc36571785b515898246, 'Fiat-backed stablecoin', 'CUSD', 18, ''),
    ('ethereum', 0xdc59ac4fefa32293a95889dc396682858d52e5db, 'Algorithmic stablecoin', 'BEAN', 6, ''),
    ('ethereum', 0xa774ffb4af6b0a91331c084e1aebae6ad535e6f3, 'Crypto-backed stablecoin', 'FLEXUSD', 18, ''),
    ('ethereum', 0xa47c8bf37f92abed4a126bda807a7b7498661acd, 'Algorithmic stablecoin', 'UST', 18, 'Wrapped UST Token'),
    ('ethereum', 0x0000000000085d4780b73119b644ae5ecd22b376, 'Fiat-backed stablecoin', 'TUSD', 18, 'TrueUSD'),
    ('ethereum', 0x4c9edd5852cd905f086c759e8383e09bff1e68b3, 'Crypto-backed stablecoin', 'USDe', 18, ''),
    ('ethereum', 0xc56c2b7e71b54d38aab6d52e94a04cbfa8f604fa, 'Fiat-backed stablecoin', 'ZUSD', 6, ''),
    ('ethereum', 0x0a5e677a6a24b2f1a2bf4f3bffc443231d2fdec8, 'Crypto-backed stablecoin', 'USX', 18, ''),
    ('ethereum', 0x196f4727526ea7fb1e17b2071b3d8eaa38486988, 'Crypto-backed stablecoin', 'RSV', 18, ''),
    ('ethereum', 0xbc6da0fe9ad5f3b0d58160288917aa56653660e9, 'Crypto-backed stablecoin', 'alUSD', 18, 'Alchemix USD'),
    ('ethereum', 0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5, 'Fiat-backed stablecoin', 'USD0', 18, 'Usual USD'),
    ('ethereum', 0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3, 'Crypto-backed stablecoin', 'MIM', 18, 'Magic Internet Money'),
    ('ethereum', 0xd5a14081a34d256711b02bbef17e567da48e80b5, 'RWA-backed stablecoin', 'wUSDR', 9, ''),
    ('ethereum', 0x57ab1ec28d129707052df4df418d58a2d46d5f51, 'Crypto-backed stablecoin', 'sUSD', 18, 'Synthetix sUSD'),
    ('ethereum', 0x056fd409e1d7a124bd7017459dfea2f387b6d5cd, 'Fiat-backed stablecoin', 'GUSD', 2, 'Gemini Dollar'),
    ('ethereum', 0x1c48f86ae57291f7686349f12601910bd8d470bb, 'Fiat-backed stablecoin', 'USDK', 18, ''),
    ('ethereum', 0x865377367054516e17014ccded1e7d814edc9ce4, 'Crypto-backed stablecoin', 'DOLA', 18, ''),
    ('ethereum', 0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'Crypto-backed stablecoin', 'BOB', 18, ''),
    ('ethereum', 0x956f47f50a910163d8bf957cf5846d573e7f87ca, 'Algorithmic stablecoin', 'FEI', 18, 'Fei USD'),
    ('ethereum', 0x9a1997c130f4b2997166975d9aff92797d5134c2, 'RWA-backed stablecoin', 'USDap', 18, ''),
    ('ethereum', 0xd46ba6d942050d489dbd938a2c909a5d5039a161, 'Crypto-backed stablecoin', 'AMPL', 9, 'Ampleforth'),
    ('ethereum', 0x03ab458634910aad20ef5f1c8ee96f1d6ac54919, 'Crypto-backed stablecoin', 'RAI', 18, 'Rai Reflex Index'),
    ('ethereum', 0x674c6ad92fd080e4004b2312b45f796a192d27a0, 'Algorithmic stablecoin', 'USDN', 18, 'Neutrino USD'),
    ('ethereum', 0x866a2bf4e572cbcf37d5071a7a58503bfb36be1b, 'Fiat-backed stablecoin', 'M', 6, 'M by M^0'),
    ('ethereum', 0x57Ab1E02fEE23774580C119740129eAC7081e9D3, 'Crypto-backed stablecoin', 'sUSD', 18, 'Synthetix sUSD'),
    ('ethereum', 0xa693b19d2931d498c5b318df961919bb4aee87a5, 'Crypto-backed stablecoin', 'UST', 6, 'UST (Wormhole)'),
    ('ethereum', 0xe07F9D810a48ab5c3c914BA3cA53AF14E4491e8A, 'Crypto-backed stablecoin', 'GYD', 18, 'Gyro Dollar'),
    ('ethereum', 0xdb25f211ab05b1c97d595516f45794528a807ad8, 'Fiat-backed stablecoin', 'EURS', 18, 'STASIS EURS')


     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
