{{ config(
      schema = 'tokens_mantle'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["mantle"]\',
                                  "sector",
                                  "tokens_mantle",
                                  \'["rantum"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('mantle', 0x201eba5cc46d216ce6dc03f6a759e8e766e956ae, 'Fiat-backed stablecoin', 'USDT', 6, ''),
    ('mantle', 0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9, 'Fiat-backed stablecoin', 'USDC', 6, ''),
    ('mantle', 0x5be26527e817998a7206475496fde1e68957c5a6, 'Fiat-backed stablecoin', 'USDY', 18, ''),
    ('mantle', 0xeb466342c4d449bc9f53a865d5cb90586f405215, 'Fiat-backed stablecoin', 'axlUSDC', 6, ''),
    ('mantle', 0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'Fiat-backed stablecoin', 'USDe', 18, '')
     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
