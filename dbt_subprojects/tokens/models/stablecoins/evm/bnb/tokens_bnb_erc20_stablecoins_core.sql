{% set chain = 'bnb' %}

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
-- new stablecoins should be added to tokens_bnb_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x14016e85a25aeb13065688cafb43044c2ef86784, 'USD'), -- TUSD
     (0x23396cf899ca06c4472205fc903bdb4de249d6fc, 'USD'), -- UST
     (0x0782b6d8c4551b9760e74c0545a9bcd90bdc41e5, 'USD'), -- HAY
     (0x90c97f71e18723b0cf0dfa30ee176ab653e89f40, 'USD'), -- FRAX
     (0x6bf2be9468314281cd28a94c35f967cafd388325, 'USD'), -- oUSD
     (0x55d398326f99059ff775485246999027b3197955, 'USD'), -- USDT
     (0xde7d1ce109236b12809c45b23d22f30dba0ef424, 'USD'), -- USDS
     (0xfa4ba88cf97e282c505bea095297786c16070129, 'USD'), -- CUSD
     (0xc5f0f7b66764f6ec8c8dff7ba683102295e16409, 'USD'), -- FDUSD
     (0x1d6cbdc6b29c6afbae65444a1f65ba9252b8ca83, 'USD'), -- TOR
     (0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'USD'), -- BOB
     (0x2f29bc0ffaf9bff337b31cbe6cb5fb3bf12e5840, 'USD'), -- DOLA
     (0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d, 'USD'), -- USDC
     (0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'USD'), -- MAI
     (0x4bd17003473389a42daf6a0a729f6fdb328bbbd7, 'USD'), -- VAI
     (0xf0186490b18cb74619816cfc7feb51cdbe4ae7b9, 'USD'), -- zUSD
     (0xfe19f0b51438fd612f6fd59c1dbb3ea319f433ba, 'USD'), -- MIM
     (0xe9e7cea3dedca5984780bafc599bd69add087d56, 'USD'), -- BUSD
     (0xb5102cee1528ce2c760893034a4603663495fd72, 'USD'), -- USX
     (0xb7f8cd00c5a06c0537e2abff0b58033d02e5e094, 'USD'), -- PAX
     (0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3, 'USD'), -- DAI
     (0xd17479997f34dd9156deef8f95a52d81d265be9c, 'USD'), -- USDD
     (0x4268b8f0b87b6eae5d897996e6b845ddbd99adf3, 'USD'), -- axlUSDC
     (0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d, 'USD'), -- USD1
     (0x71be881e9c5d4465b3fff61e89c6f3651e69b5bb, 'BRL'), -- BRZ
     (0xb6bb22f4d1e58e9e43efa2ec7f572d215b3cf08a, 'BRL'), -- BBRL
     (0xa8aea66b361a8d53e8865c62d142167af28af058, 'NGN'), -- cNGN
     (0xa40640458fbc27b6eefedea1e9c9e17d4cee7a21, 'EUR'), -- AEUR
     (0x12f31b73d812c6bb0d735a218c086d44d5fe5f89, 'EUR'), -- agEUR
     (0x9d1a7a3191102e9f900faa10540837ba84dcbae7, 'EUR'), -- EURI
     (0x66207e39bb77e6b99aab56795c7c340c08520d83, 'IDR'), -- IDRT
     (0x649a2da7b28e0d54c13d5eff95d3a660652742cc, 'IDR'), -- IDRX
     (0x2074c8e9253cd50d3cb81deb28ae85d932d2d26b, 'ZAR'), -- xZAR
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'USD'), -- USDe

     (0xaf44a1e76f56ee12adbb7ba8acd3cbd474888122, 'USD'), -- DUSD
     (0x17eafd08994305d8ace37efb82f1523177ec70ee, 'USD'), -- USDA
     (0x5a110fc00474038f6c02e89c707d638602ea44b5, 'USD'), -- USDF
     (0xb3b02e4a9fb2bd28cc2ff97b0ab3f6b3ec1ee9d2, 'USD'), -- USDf
     (0x40af3827f39d0eacbf4a168f8d4ee67c121d11c9, 'USD'), -- TUSD
     (0x2492d0006411af6c8bbb1c8afc1b0197350a79e9, 'USD'), -- USR
     (0xffffff9936bd58a008855b0812b44d2c8dffe2aa, 'USD'), -- GGUSD
     (0xb4818bb69478730ef4e33cc068dd94278e2766cb, 'USD'), -- satUSD
     (0x45e51bc23d592eb2dba86da3985299f7895d66ba, 'USD'), -- USDD
     (0xa228d4546eebafd9808ede3f4b490fd4ae83fb74, 'USD')  -- USDA

) as temp_table (contract_address, currency)
