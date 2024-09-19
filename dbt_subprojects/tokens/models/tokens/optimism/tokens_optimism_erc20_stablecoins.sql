{{ config(
      schema = 'tokens_optimism'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "sector",
                                  "tokens_optimism",
                                  \'["msilb7", "synthquest"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('optimism', 0xfb21b70922b9f6e3c6274bcd6cb1aa8a0fe20b80, 'Algorithmic stablecoin', 'UST', 6, 'Terra USD'),
    ('optimism', 0x970d50d09f3a656b43e11b0d45241a84e3a6e011, 'Crypto-backed stablecoin', 'DAI+', 18, 'DAI+'),
    ('optimism', 0xdfa46478f9e5ea86d57387849598dbfb2e964b02, 'Crypto-backed stablecoin', 'MAI', 18, 'Mai Stablecoin'),
    ('optimism', 0x3666f603cc164936c1b87e207f36beba4ac5f18a, 'Bridge-backed stablecoin', 'hUSD', 6, 'USD Coin Hop Token'),
    ('optimism', 0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9, 'Crypto-backed stablecoin', 'sUSD', 18, 'Synth sUSD'),
    ('optimism', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1, 'Crypto-backed stablecoin', 'DAI', 18, 'Dai Stablecoin'),
    ('optimism', 0xc40f949f8a4e094d1b49a23ea9241d289b7b2819, 'Crypto-backed stablecoin', 'LUSD', 18, 'LUSD Stablecoin'),
    ('optimism', 0x25d8039bb044dc227f741a9e381ca4ceae2e6ae8, 'Bridge-backed stablecoin', 'hUSDC', 6, 'USD Coin Hop Token'),
    ('optimism', 0x79af5dd14e855823fa3e9ecacdf001d99647d043, 'Crypto-backed stablecoin', 'EUR', 18, 'Jarvis Synthetic Euro'),
    ('optimism', 0xfbc4198702e81ae77c06d58f81b629bdf36f0a71, 'Crypto-backed stablecoin', 'sEUR', 18, 'Synth sEUR'),
    ('optimism', 0xcb59a0a753fdb7491d5f3d794316f1ade197b21e, 'Fiat-backed stablecoin', 'TUSD', 18, 'TrueUSD'),
    ('optimism', 0x7fb688ccf682d58f86d7e38e03f9d22e7705448b, 'Crypto-backed stablecoin', 'RAI', 18, 'Rai Reflex Index'),
    ('optimism', 0xcb8fa9a76b8e203d8c3797bf438d8fb81ea3326a, 'Algorithmic stablecoin', 'alUSD', 18, 'Alchemix USD'),
    ('optimism', 0x340fe1d898eccaad394e2ba0fc1f93d27c7b717a, 'Algorithmic stablecoin', 'wUSDR', 9, 'Wrapped USDR'),
    ('optimism', 0x2e3d870790dc77a83dd1d18184acc7439a53f475, 'Hybrid stablecoin', 'FRAX', 18, 'FRAX'),
    ('optimism', 0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'Crypto-backed stablecoin', 'BOB', 18, 'BOB'),
    ('optimism', 0x8ae125e8653821e851f12a49f7765db9a9ce7384, 'Crypto-backed stablecoin', 'DOLA', 18, 'Dola USD Stablecoin'),
    ('optimism', 0x0b2c639c533813f4aa9d7837caf62653d097ff85, 'Fiat-backed stablecoin', 'USDC', 6, 'Circle USDC'),
    ('optimism', 0x67c10c397dd0ba417329543c1a40eb48aaa7cd00, 'Bridge-backed stablecoin', 'nUSD', 18, 'Synapse USD'),
    ('optimism', 0x56900d66d74cb14e3c86895789901c9135c95b16, 'Bridge-backed stablecoin', 'hDAI', 18, 'DAI Hop Token'),
    ('optimism', 0x73cb180bf0521828d8849bc8cf2b920918e23032, 'Crypto-backed stablecoin', 'USD+', 6, 'USD+'),
    ('optimism', 0x59d9356e565ab3a36dd77763fc0d87feaf85508c, 'Fiat-backed stablecoin', 'USDM', 18, ''),
    ('optimism', 0xb153fb3d196a8eb25522705560ac152eeec57901, 'Crypto-backed stablecoin', 'MIM', 18, 'Magic Internet Money'),
    ('optimism', 0x94b008aa00579c1307b0ef2c499ad98a8ce58e58, 'Fiat-backed stablecoin', 'USDT', 6, 'Tether USD'),
    ('optimism', 0x7f5c764cbc14f9669b88837ca1490cca17c31607, 'Fiat-backed stablecoin', 'USDC.e', 6, 'USD Coin'),
    ('optimism', 0xa3a538ea5d5838dc32dde15946ccd74bdd5652ff, 'Crypto-backed stablecoin', 'sINR', 18, 'Synth sINR'),
    ('optimism', 0x9c9e5fd8bbc25984b178fdce6117defa39d2db39, 'Fiat-backed stablecoin', 'BUSD', 18, 'Binance-Peg BUSD Token'),
    ('optimism', 0x7113370218f31764c1b6353bdf6004d86ff6b9cc, 'Algorithmic stablecoin', 'USDD', 18, 'Decentralized USD'),
    ('optimism', 0x2057c8ecb70afd7bee667d76b4cd373a325b1a20, 'Bridge-backed stablecoin', 'hUSDT', 6, 'Tether USD Hop Token'),
    ('optimism', 0x9485aca5bbbe1667ad97c7fe7c4531a624c8b1ed, 'Crypto-backed stablecoin', 'agEUR', 18, 'agEUR'),
    ('optimism', 0xbfd291da8a403daaf7e5e9dc1ec0aceacd4848b9, 'Crypto-backed stablecoin', 'USX', 18, 'dForce USD'),
    ('optimism', 0xba28feb4b6a6b81e3f26f08b83a19e715c4294fd, 'Algorithmic stablecoin', 'UST', 6, 'UST (Wormhole)')


     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
