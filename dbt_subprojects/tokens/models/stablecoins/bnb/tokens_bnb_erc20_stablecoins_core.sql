{% set chain = 'bnb' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_bnb_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, backing, symbol, decimals, name
from (values

     (0x14016e85a25aeb13065688cafb43044c2ef86784, 'Fiat-backed stablecoin', 'TUSD', 18, ''),
     (0x23396cf899ca06c4472205fc903bdb4de249d6fc, 'Algorithmic stablecoin', 'UST', 18, ''),
     (0x0782b6d8c4551b9760e74c0545a9bcd90bdc41e5, 'Crypto-backed stablecoin', 'HAY', 18, ''),
     (0x90c97f71e18723b0cf0dfa30ee176ab653e89f40, 'Hybrid stablecoin', 'FRAX', 18, ''),
     (0x6bf2be9468314281cd28a94c35f967cafd388325, 'Hybrid stablecoin', 'oUSD', 18, ''),
     (0x55d398326f99059ff775485246999027b3197955, 'Fiat-backed stablecoin', 'USDT', 18, ''),
     (0xde7d1ce109236b12809c45b23d22f30dba0ef424, 'Hybrid stablecoin', 'USDS', 18, ''),
     (0xfa4ba88cf97e282c505bea095297786c16070129, 'Fiat-backed stablecoin', 'CUSD', 18, ''),
     (0xc5f0f7b66764f6ec8c8dff7ba683102295e16409, 'Fiat-backed stablecoin', 'FDUSD', 18, ''),
     (0x2952beb1326accbb5243725bd4da2fc937bca087, 'RWA-backed stablecoin', 'wUSDR', 9, ''),
     (0x1d6cbdc6b29c6afbae65444a1f65ba9252b8ca83, 'Crypto-backed stablecoin', 'TOR', 18, ''),
     (0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'Crypto-backed stablecoin', 'BOB', 18, ''),
     (0x6458df5d764284346c19d88a104fd3d692471499, 'Hybrid stablecoin', 'iUSDS', 18, ''),
     (0x2f29bc0ffaf9bff337b31cbe6cb5fb3bf12e5840, 'Crypto-backed stablecoin', 'DOLA', 18, ''),
     (0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d, 'Fiat-backed stablecoin', 'USDC', 18, ''),
     (0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'Crypto-backed stablecoin', 'MAI', 18, ''),
     (0x4bd17003473389a42daf6a0a729f6fdb328bbbd7, 'Crypto-backed stablecoin', 'VAI', 18, ''),
     (0xf0186490b18cb74619816cfc7feb51cdbe4ae7b9, 'RWA-backed stablecoin', 'zUSD', 18, ''),
     (0xfe19f0b51438fd612f6fd59c1dbb3ea319f433ba, 'Crypto-backed stablecoin', 'MIM', 18, ''),
     (0xe9e7cea3dedca5984780bafc599bd69add087d56, 'Fiat-backed stablecoin', 'BUSD', 18, ''),
     (0xb5102cee1528ce2c760893034a4603663495fd72, 'Crypto-backed stablecoin', 'USX', 18, ''),
     (0xb7f8cd00c5a06c0537e2abff0b58033d02e5e094, 'Crypto-backed stablecoin', 'PAX', 18, ''),
     (0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3, 'Hybrid stablecoin', 'DAI', 18, ''),
     (0xd17479997f34dd9156deef8f95a52d81d265be9c, 'Algorithmic stablecoin', 'USDD', 18, ''),
     (0x4268b8f0b87b6eae5d897996e6b845ddbd99adf3, 'Crypto-backed stablecoin', 'axlUSDC', 6, ''),
     (0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d, 'Fiat-backed stablecoin', 'USD1', 18, ''),
     (0x71be881e9c5d4465b3fff61e89c6f3651e69b5bb, 'Fiat-backed stablecoin', 'BRZ', 18, ''),
     (0xb6bb22f4d1e58e9e43efa2ec7f572d215b3cf08a, 'Fiat-backed stablecoin', 'BBRL', 18, ''),
     (0xa8aea66b361a8d53e8865c62d142167af28af058, 'Fiat-backed stablecoin', 'cNGN', 18, ''),
     (0xa40640458fbc27b6eefedea1e9c9e17d4cee7a21, 'Fiat-backed stablecoin', 'AEUR', 18, ''),
     (0x12f31b73d812c6bb0d735a218c086d44d5fe5f89, 'Crypto-backed stablecoin', 'agEUR', 18, ''),
     (0x9d1a7a3191102e9f900faa10540837ba84dcbae7, 'Fiat-backed stablecoin', 'EURI', 18, ''),
     (0x66207e39bb77e6b99aab56795c7c340c08520d83, 'Fiat-backed stablecoin', 'IDRT', 18, ''),
     (0x649a2da7b28e0d54c13d5eff95d3a660652742cc, 'Fiat-backed stablecoin', 'IDRX', 18, ''),
     (0x2074c8e9253cd50d3cb81deb28ae85d932d2d26b, 'Fiat-backed stablecoin', 'xZAR', 18, ''),
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'Crypto-backed stablecoin', 'USDe', 18, 'Ethena')

) as temp_table (contract_address, backing, symbol, decimals, name)
