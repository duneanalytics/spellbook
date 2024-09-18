{{ config(
      schema = 'tokens_base'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["base"]\',
                                  "sector",
                                  "tokens_base",
                                  \'["synthquest"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES

    ('base', 0x60a3e35cc302bfa44cb288bc5a4f316fdb1adb42, 'Fiat-backed stablecoin', 'EURC', 6, ''),
    ('base', 0xb79dd08ea68a908a97220c76d19a6aa9cbde4376, 'Crypto-backed stablecoin', 'USD+', 6, ''),
    ('base', 0xcc7ff230365bd730ee4b352cc2492cedac49383e, 'Algorithmic stablecoin', 'hyUSD', 18, ''),
    ('base', 0xcfa3ef56d303ae4faaba0592388f19d7c3399fb4, 'Crypto-backed stablecoin', 'eUSD', 18, ''),
    ('base', 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 'Fiat-backed stablecoin', 'USDC', 6, ''),
    ('base', 0x04d5ddf5f3a8939889f11e97f8c4bb48317f1938, 'Fiat-backed stablecoin', 'USDz', 18, ''),
    ('base', 0x4621b7a9c75199271f773ebd9a499dbd165c3191, 'Crypto-backed stablecoin', 'DOLA', 18, ''),
    ('base', 0xca72827a3d211cfd8f6b00ac98824872b72cab49, 'Fiat-backed stablecoin', 'cgUSD', 6, '')


     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
