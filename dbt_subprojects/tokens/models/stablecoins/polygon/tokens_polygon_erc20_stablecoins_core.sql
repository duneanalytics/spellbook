{% set chain = 'polygon' %}

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
-- new stablecoins should be added to tokens_polygon_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x2791bca1f2de4661ed88a30c99a7a9449aa84174), -- USDC
     (0x3c499c542cef5e3811e1192ce70d8cc03d5c3359), -- USDC
     (0x692597b009d13c4049a947cab2239b7d6517875f), -- UST
     (0xcf66eb3d546f0415b368d98a95eaf56ded7aa752), -- USX
     (0x8f3cf7ad23cd3cadbd9735aff958023239c6a063), -- DAI
     (0x2e1ad108ff1d8c782fcbbb89aad783ac49586756), -- TUSD
     (0xd86b5923f3ad7b585ed81b448170ae026c65ae9a), -- IRON
     (0xffa4d863c96e743a2e1513824ea006b8d0353c57), -- USDD
     (0x9c9e5fd8bbc25984b178fdce6117defa39d2db39), -- BUSD
     (0x49a0400587a7f65072c87c4910449fdcc5c47242), -- MIM
     (0x2f1b1662a895c6ba01a99dcaf56778e7d77e5609), -- USDS
     (0xaf0d9d65fc54de245cda37af3d18cbec860a4d4b), -- wUSDR
     (0x45c32fa6df82ead1e2ef74d17b76547eddfaff89), -- FRAX
     (0x66f31345cb9477b427a1036d43f923a557c432a4), -- iUSDS
     (0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b), -- BOB
     (0xc2132d05d31c914a87c6611c10748aeb04b58e8f), -- USDT
     (0xa3fa99a148fa48d14ed51d610c367c61876997f1), -- miMATIC
     (0xdab529f40e671a1d4bf91361c21bf9f0c9712ab7), -- BUSD
     (0x3a3e7650f8b9f667da98f236010fbf44ee4b2975), -- xUSD
     (0x23001f892c0c82b79303edc9b9033cd190bb21c7), -- LUSD
     (0x750e4c4984a9e0f12978ea6742bc1c5d248f40ed), -- axlUSDC
     (0x2893Ef551B6dD69F661Ac00F11D93E5Dc5Dc0e99), -- BUIDL
     (0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a), -- AUSD
     (0x5c067c80c00ecd2345b05e83a3e758ef799c40b5), -- BRL1
     (0xe6a537a407488807f0bbeb0038b79004f19dddfb), -- BRLA
     (0x4ed141110f6eeeaba9a1df36d8c26f684d2475dc), -- BRZ
     (0x4d1137c03262bcd286f26782033b60af792cd59d), -- CLPC
     (0x52828daa48c1a9a06f37500882b42daf0be04c3b), -- cNGN
     (0x12050c705152931cfee3dd56c52fb09dea816c23), -- COPM
     (0xc2ff25dd99e467d2589b2c26edd270f220f14e47), -- dEURO
     (0x888883b5f5d21fb10dfeb70e8f9722b9fb0e5e51), -- EUROP
     (0x4933a85b5b5466fbaf179f72d3de273c287ec2c2), -- EURAU
     (0xe0b52e49357fd4daf2c15e02058dce6bc0057db4), -- agEUR
     (0xe0aea583266584dafbb3f9c3211d5588c73fea8d), -- EURe
     (0xe111178a87a3bff0c8d18decba5798827539ae99), -- EURS
     (0x554cd6bdd03214b10aafa3e0d4d42de0c5d2937b), -- IDRT
     (0x649a2da7b28e0d54c13d5eff95d3a660652742cc), -- IDRX
     (0xdc3326e71d45186f113a2f448984ca0e8d201995), -- XSGD
     (0xe4095d9372e68d108225c306a4491cacfb33b097), -- VEUR
     (0x9de41aff9f55219d5bf4359f167d1d0c772a396d), -- CADC
     (0x2c826035c1c36986117a0e949bd6ad4bab54afe2), -- XIDR
     (0xe7c3d8c9a439fede00d2600032d5db0be71c3c29), -- JPYC
     (0x30de46509dbc3a491128f97be0aaf70dc7ff33cb), -- xZAR
     (0xb755506531786c8ac63b756bab1ac387bacb0c04), -- ZARP

     (0xffffff9936bd58a008855b0812b44d2c8dffe2aa)  -- GGUSD

) as temp_table (contract_address)
