{{
    config(
        schema = 'tokens'
        , alias = 'stablecoins'
        , materialized = 'view'
        , post_hook='{{ expose_spells(blockchains = \'["arbitrum","avalanche_c","base","blast","bnb","celo","ethereum","fantom","gnosis","linea","optimism","polygon","scroll","tron","zkevm","zksync"]\',
                        spell_type = "sector",
                        spell_name = "tokens",
                        contributors = \'["thetroyharris", "gentrexha", "dot2dotseurat", "msilb7", "lgingerich", "Henrystats"]\') }}'
    )
}}

SELECT contract_address, symbol, decimals, 'arbitrum' AS blockchain 
FROM (VALUES
    (0xaf88d065e77c8cc2239327c5edb3a432268e5831, 'USDC', 6) -- Native
    , (0xff970a61a04b1ca14834a43f5de4533ebddb5cc8, 'USDC', 6) -- Bridged
    , (0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9, 'USDT', 6)
    , (0xda10009cbd5d07dd0cecc66161fc93d7c9000da1, 'DAI', 18)
    , (0x17fc002b466eec40dae837fc4be5c67993ddbd6f, 'FRAX', 18)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL
SELECT contract_address, symbol, decimals, 'avalanche_c' AS blockchain 
FROM (VALUES
    (0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E, 'USDC', 6)
    , (0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664, 'USDC', 6) --bridged
    , (0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7, 'USDT', 6)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL
SELECT contract_address, symbol, decimals, 'base' AS blockchain 
FROM (VALUES
    (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 'USDC', 6)
    , (0xb79dd08ea68a908a97220c76d19a6aa9cbde4376, 'USD+', 6)
    , (0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 'DAI',  18)
    , (0x4a3a6dd60a34bb2aba60d73b4c88315e9ceb6a3d, 'MIM',  18)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL
SELECT contract_address, symbol, decimals, 'blast' AS blockchain 
FROM (VALUES
    (0x4300000000000000000000000000000000000003, 'USDB', 18)
    , (0x76da31d7c9cbeae102aff34d3398bc450c8374c1, 'MIM',  18)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL
SELECT contract_address, symbol, decimals, 'bnb' AS blockchain 
FROM (VALUES
    (0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d, 'USDC', 18)
    , (0x55d398326f99059ff775485246999027b3197955, 'USDT', 18)
    , (0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3, 'DAI',  18)
    , (0xd17479997F34dd9156Deef8F95A52D81D265be9c, 'USDD', 18)
    , (0xe80772eaf6e2e18b651f160bc9158b2a5cafca65, 'USD+', 6)
    , (0xb7f8cd00c5a06c0537e2abff0b58033d02e5e094, 'PAX',  18)
    , (0x8965349fb649a33a30cbfda057d8ec2c48abe2a2, 'USDC', 18)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL
SELECT contract_address, symbol, decimals, 'celo' AS blockchain 
FROM (VALUES
    (0xceba9300f2b948710d2653dd7b07f33a8b32118c, 'USDC', 6) --native
    , (0xef4229c8c3250c675f21bcefa42f58efbff6002a, 'USDC', 6) --bridged
    , (0x37f750b7cc259a2f741af45294f6a16572cf5cad, 'USDC', 6) --bridged
    , (0x48065fbBE25f71C9282ddf5e1cD6D6A887483D5e, 'USDT', 6) --native
    , (0x617f3112bf5397d0467d315cc709ef968d9ba546, 'USDT', 6) --bridged
    , (0x765de816845861e75a25fca122bb6898b8b1282a, 'cUSD', 18)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL
SELECT contract_address, symbol, decimals, 'ethereum' AS blockchain 
FROM (VALUES
    (0xbc6da0fe9ad5f3b0d58160288917aa56653660e9, 'alUSD', 18)
    , (0xd46ba6d942050d489dbd938a2c909a5d5039a161, 'AMPL',  9)
    , (0x4fabb145d64652a948d72533023f6e7a623c7c53, 'BUSD',  18)
    , (0x6b175474e89094c44da98b954eedeac495271d0f, 'DAI',   18)
    , (0xdb25f211ab05b1c97d595516f45794528a807ad8, 'EURS',  18)
    , (0x956f47f50a910163d8bf957cf5846d573e7f87ca, 'FEI',   18)
    , (0x853d955acef822db058eb8505911ed77f175b99e, 'FRAX',  18)
    , (0x056fd409e1d7a124bd7017459dfea2f387b6d5cd, 'GUSD',	2)
    , (0xdf574c24545e5ffecb9a659c229253d4111d87e1, 'HUSD',	8)
    , (0x5f98805A4E8be255a32880FDeC7F6728C6568bA0, 'LUSD',	18)
    , (0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3, 'MIM',   18)
    , (0xe2f2a5c287993345a840db3b0845fbc70f5935a5, 'MUSD',  18)
    , (0x8e870d67f660d95d5be530380d0ec0bd388289e1, 'PAX',   18)
    , (0x03ab458634910aad20ef5f1c8ee96f1d6ac54919, 'RAI',   18)
    , (0x57Ab1ec28D129707052df4dF418D58a2D46d5f51, 'sUSD',  18)
    , (0x57Ab1E02fEE23774580C119740129eAC7081e9D3, 'sUSD',  18)
    , (0x0000000000085d4780b73119b644ae5ecd22b376, 'TUSD',  18)
    , (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 'USDC',	6)
    , (0x674c6ad92fd080e4004b2312b45f796a192d27a0, 'USDN',	18)
    , (0x1456688345527bE1f37E9e627DA0837D6f08C925, 'USDP',	18)
    , (0xdac17f958d2ee523a2206206994597c13d831ec7, 'USDT',	6)
    , (0xa47c8bf37f92abed4a126bda807a7b7498661acd, 'UST',   18)
    , (0xa693b19d2931d498c5b318df961919bb4aee87a5, 'UST',   6)
    , (0xe07F9D810a48ab5c3c914BA3cA53AF14E4491e8A, 'GYD',   18)
    , (0x4c9edd5852cd905f086c759e8383e09bff1e68b3, 'USDe',  18)
    , (0xf939e0a03fb07f59a73314e73794be0e57ac1b4e, 'crvUSD',18)
    , (0xc5f0f7b66764f6ec8c8dff7ba683102295e16409, 'FDUSD', 18)
    , (0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f, 'GHO',   18)
    , (0x865377367054516e17014CcdED1e7d814EDC9ce4, 'DOLA',  18)
    , (0x6c3ea9036406852006290770bedfcaba0e23a0e8, 'PYUSD', 6)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL
SELECT contract_address, symbol, decimals, 'fantom' AS blockchain 
FROM (VALUES
    (0xc54A1684fD1bef1f077a336E6be4Bd9a3096a6Ca, '2SHARES', 18)
    , (0x04068DA6C83AFCFA0e13ba15A6696662335D5B75, 'USDC', 6)
    , (0x049d68029688eAbF473097a2fC38ef61633A3C7A, 'fUSDT', 6)
    , (0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E, 'DAI', 18)
    , (0xdc301622e621166BD8E82f2cA0A26c13Ad0BE355, 'FRAX', 18)
    , (0xAd84341756Bf337f5a0164515b1f6F993D194E1f, 'FUSD', 18)
    , (0x7a6e4E3CC2ac9924605DCa4bA31d1831c84b44aE, '2OMB', 18)
    , (0x846e4D51d7E2043C1a87E0Ab7490B93FB940357b, 'UST', 6)
    , (0x87a5C9B60A3aaf1064006FE64285018e50e0d020, 'MAGIK', 18)
    , (0x9879aBDea01a879644185341F7aF7d8343556B7a, 'TUSD', 18)
    , (0x1D3918043d22de2D799a4d80f72Efd50Db90B5Af, 'sPDO', 18)
    , (0x5f0456F728E2D59028b4f5B8Ad8C604100724C6A, 'L3USD', 18)
    , (0xb9D62c829fbF7eAff1EbA4E50F3D0480b66c1748, 'PDO', 18)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL
SELECT contract_address, symbol, decimals, 'gnosis' AS blockchain 
FROM (VALUES
    (0xddafbb505ad214d7b80b1f830fccc89b60fb7a83, 'USDC', 6)
    , (0x4ecaba5870353805a9f068101a40e0f32ed605c6, 'USDT', 6)
    , (0xe91d153e0b41518a2ce8dd3d7944fa863463a97d, 'WXDAI', 18)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL
SELECT contract_address, symbol, decimals, 'linea' AS blockchain 
FROM (VALUES
    (0x176211869ca2b568f2a7d4ee941e073a821ee1ff,    'USDC', 6,  'USD Coin') --bridged
    , (0xb79dd08ea68a908a97220c76d19a6aa9cbde4376,  'USD+', 6,  'Overnight USD')
    , (0xA219439258ca9da29E9Cc4cE5596924745e12B93,  'USDT',  6, 'Tether')
    , (0x4af15ec2a0bd43db75dd04e62faa3b8ef36b00d5,  'DAI',  18, 'Dai')
    , (0xd2bc272EA0154A93bf00191c8a1DB23E67643EC5,  'USDP',  18, 'Pax Dollar')
    , (0xDD3B8084AF79B9BaE3D1b668c0De08CCC2C9429A,  'MIM',  18, 'Magic Internet Money')
) AS temp_table (contract_address, symbol, decimals, name)
UNION ALL
SELECT contract_address, symbol, decimals, 'optimism' AS blockchain 
FROM (VALUES
      --Type sourced from Defillama's mappings when relevant: https://defillama.com/stablecoins
       (0xda10009cbd5d07dd0cecc66161fc93d7c9000da1, 'DAI', 18, 'Dai Stablecoin', 'USD', 'Crypto-Backed')
      ,(0x94b008aa00579c1307b0ef2c499ad98a8ce58e58, 'USDT', 6, 'Tether USD', 'USD', 'Fiat-Backed')
      ,(0x7f5c764cbc14f9669b88837ca1490cca17c31607, 'USDC', 6, 'USD Coin', 'USD', 'Fiat-Backed')
      ,(0xc40f949f8a4e094d1b49a23ea9241d289b7b2819, 'LUSD', 18, 'LUSD Stablecoin', 'USD', 'Crypto-Backed')
      ,(0xbfd291da8a403daaf7e5e9dc1ec0aceacd4848b9, 'USX', 18, 'dForce USD', 'USD', 'Crypto-Backed')
      ,(0x2e3d870790dc77a83dd1d18184acc7439a53f475, 'FRAX', 18, 'FRAX', 'USD', 'Algorithmic')
      ,(0xfb21b70922b9f6e3c6274bcd6cb1aa8a0fe20b80, 'UST', 6, 'Terra USD', 'USD', 'Algorithmic')
      ,(0x7113370218f31764c1b6353bdf6004d86ff6b9cc, 'USDD', 18, 'Decentralized USD','USD','Algorithmic')
      ,(0xcb59a0a753fdb7491d5f3d794316f1ade197b21e, 'TUSD', 18, 'TrueUSD','USD','Fiat-Backed')
      ,(0xcb8fa9a76b8e203d8c3797bf438d8fb81ea3326a, 'alUSD', 18, 'Alchemix USD','USD','Algorithmic')
      ,(0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'BOB', 18, 'BOB','USD','Crypto-Backed')
      ,(0xdfa46478f9e5ea86d57387849598dbfb2e964b02, 'MAI', 18, 'Mai Stablecoin','USD','Crypto-Backed')
      ,(0x7fb688ccf682d58f86d7e38e03f9d22e7705448b, 'RAI', 18, 'Rai Reflex Index','None','Crypto-Backed')
      ,(0x9c9e5fd8bbc25984b178fdce6117defa39d2db39, 'BUSD', 18, 'Binance-Peg BUSD Token','USD','Fiat-Backed')
      ,(0xb153fb3d196a8eb25522705560ac152eeec57901, 'MIM', 18, 'Magic Internet Money','USD','Crypto-Backed')
      ,(0x8ae125e8653821e851f12a49f7765db9a9ce7384, 'DOLA', 18, 'Dola USD Stablecoin','USD','Crypto-Backed')
      ,(0x73cb180bf0521828d8849bc8CF2B920918e23032, 'USD+', 6, 'USD+', 'USD','Crypto-Backed')
      ,(0x9485aca5bbbe1667ad97c7fe7c4531a624c8b1ed, 'agEUR', 18, 'agEUR', 'EUR', 'Crypto-Backed')
      ,(0x79af5dd14e855823fa3e9ecacdf001d99647d043, 'jEUR', 18, 'Jarvis Synthetic Euro','EUR','Crypto-Backed')
      -- --Synthetix Tokens
      ,(0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9, 'sUSD', 18, 'Synth sUSD', 'USD', 'Crypto-Backed')
      ,(0xfbc4198702e81ae77c06d58f81b629bdf36f0a71, 'sEUR', 18, 'Synth sEUR', 'EUR', 'Crypto-Backed')
      ,(0xa3a538ea5d5838dc32dde15946ccd74bdd5652ff, 'sINR', 18, 'Synth sINR', 'INR', 'Crypto-Backed')
      -- --Transfer Tokens (Common Among Bridges)
      ,(0x25d8039bb044dc227f741a9e381ca4ceae2e6ae8, 'USDC', 6, 'USD Coin Hop Token', 'USD', 'Bridge-Backed')
      ,(0x2057c8ecb70afd7bee667d76b4cd373a325b1a20, 'USDT', 6, 'Tether USD Hop Token', 'USD', 'Bridge-Backed')
      ,(0x56900d66d74cb14e3c86895789901c9135c95b16, 'DAI', 18, 'DAI Hop Token', 'USD', 'Bridge-Backed')
      ,(0x67c10c397dd0ba417329543c1a40eb48aaa7cd00, 'nUSD', 18, 'Synapse USD', 'USD', 'Bridge-Backed')
) AS tbl (contract_address, symbol, decimals, name, currency_peg, reserve_type)
UNION ALL
SELECT contract_address, symbol, decimals, 'polygon' AS blockchain 
FROM (VALUES
    (0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359,    'USDC',     6) --native 
    , (0x2791bca1f2de4661ed88a30c99a7a9449aa84174,  'USDC',     6) --bridged 
    , (0xc2132d05d31c914a87c6611c10748aeb04b58e8f,  'USDT',     6) --unkknown
    , (0x8f3cf7ad23cd3cadbd9735aff958023239c6a063,  'DAI',      18) --unkknown
    , (0x45c32fa6df82ead1e2ef74d17b76547eddfaff89,  'FRAX',     18) --unkknown
    , (0xdab529f40e671a1d4bf91361c21bf9f0c9712ab7,  'BUSD',     18) --unkknown
    , (0xc4Ce1D6F5D98D65eE25Cf85e9F2E9DcFEe6Cb5d6,  'crvUSD',   18) --unkknown
    , (0x2e1ad108ff1d8c782fcbbb89aad783ac49586756,  'TUSD',     18) --unkknown
    , (0x236eec6359fb44cce8f97e99387aa7f8cd5cde1f,  'USD+',     6) --unkknown
) AS temp_table (contract_address, symbol, decimals)
UNION ALL
SELECT contract_address, symbol, decimals, 'scroll' AS blockchain 
FROM (VALUES
    (0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4, 'USDC', 6)
    , (0xf55bec9cafdbe8730f096aa55dad6d22d44099df, 'USDT', 6)
    , (0xca77eb3fefe3725dc33bccb54edefc3d9f764f97, 'DAI',  18)   
    , (0xedeabc3a1e7d21fe835ffa6f83a710c70bb1a051, 'LUSD', 18)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL 
SELECT contract_address, symbol, decimals, 'tron' AS blockchain 
FROM (VALUES
    (0xa614f803b6fd780986a42c78ec9c7f77e6ded13c, 'USDT', 6)
    , (0x94f24e992ca04b49c6f2a2753076ef8938ed4daa, 'USDD', 18)
    , (0x3487b63d30b5b2c87fb7ffa8bcfade38eaac1abe, 'USDC', 6)
    , (0xcebde71077b830b958c8da17bcddeeb85d0bcf25, 'TUSD', 18)
    , (0x83c91bfde3e6d130e286a3722f171ae49fb25047, 'BUSD', 18)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL 
SELECT contract_address, symbol, decimals, 'zkevm' AS blockchain 
FROM (VALUES
    (0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035,    'USDC', 6)
    , (0x37eaa0ef3549a5bb7d431be78a3d99bd360d19e5,  'USDC', 6)
    , (0x1e4a5963abfd975d8c9021ce480b42188849d41d,  'USDT', 6)
    , (0xc5015b9d9161dca7e18e32f6f25c4ad850731fd4,  'DAI',  18)
    , (0xFf8544feD5379D9ffa8D47a74cE6b91e632AC44D,  'FRAX',  18)
) AS temp_table (contract_address, symbol, decimals)
UNION ALL 
SELECT contract_address, symbol, decimals, 'zksync' AS blockchain 
FROM (VALUES
    (0x1d17CBcF0D6D143135aE902365D2E5e2A16538D4, 'USDC', 6) --native
    , (0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4, 'USDC', 6) --bridged
    , (0x4B9eb6c0b6ea15176BBF62841C6B2A8a398cb656, 'DAI', 18)
    , (0x503234F203fC7Eb888EEC8513210612a43Cf6115, 'LUSD', 18)
) AS temp_table (contract_address, symbol, decimals)

