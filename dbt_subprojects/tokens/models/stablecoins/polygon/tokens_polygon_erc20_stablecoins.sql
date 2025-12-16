{% set chain = 'polygon' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

select '{{chain}}' as blockchain, contract_address, backing, symbol, decimals, name
from (values

     (0x2791bca1f2de4661ed88a30c99a7a9449aa84174, 'Fiat-backed stablecoin', 'USDC', 6, ''),
     (0x3c499c542cef5e3811e1192ce70d8cc03d5c3359, 'Fiat-backed stablecoin', 'USDC', 6, ''),
     (0x692597b009d13c4049a947cab2239b7d6517875f, 'Algorithmic stablecoin', 'UST', 18, ''),
     (0xcf66eb3d546f0415b368d98a95eaf56ded7aa752, 'Crypto-backed stablecoin', 'USX', 18, ''),
     (0x8f3cf7ad23cd3cadbd9735aff958023239c6a063, 'Hybrid stablecoin', 'DAI', 18, ''),
     (0x2e1ad108ff1d8c782fcbbb89aad783ac49586756, 'Fiat-backed stablecoin', 'TUSD', 18, ''),
     (0xd86b5923f3ad7b585ed81b448170ae026c65ae9a, 'Hybrid stablecoin', 'IRON', 18, ''),
     (0xffa4d863c96e743a2e1513824ea006b8d0353c57, 'Algorithmic stablecoin', 'USDD', 18, ''),
     (0x9c9e5fd8bbc25984b178fdce6117defa39d2db39, 'Fiat-backed stablecoin', 'BUSD', 18, ''),
     (0x49a0400587a7f65072c87c4910449fdcc5c47242, 'Crypto-backed stablecoin', 'MIM', 18, ''),
     (0x2f1b1662a895c6ba01a99dcaf56778e7d77e5609, 'Hybrid stablecoin', 'USDS', 18, ''),
     (0xaf0d9d65fc54de245cda37af3d18cbec860a4d4b, 'RWA-backed stablecoin', 'wUSDR', 9, ''),
     (0x45c32fa6df82ead1e2ef74d17b76547eddfaff89, 'Hybrid stablecoin', 'FRAX', 18, ''),
     (0x66f31345cb9477b427a1036d43f923a557c432a4, 'Hybrid stablecoin', 'iUSDS', 18, ''),
     (0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'Crypto-backed stablecoin', 'BOB', 18, ''),
     (0xc2132d05d31c914a87c6611c10748aeb04b58e8f, 'Fiat-backed stablecoin', 'USDT', 6, ''),
     (0xa3fa99a148fa48d14ed51d610c367c61876997f1, 'Crypto-backed stablecoin', 'miMATIC', 18, ''),
     (0xdab529f40e671a1d4bf91361c21bf9f0c9712ab7, 'Fiat-backed stablecoin', 'BUSD', 18, ''),
     (0x3a3e7650f8b9f667da98f236010fbf44ee4b2975, 'Crypto-backed stablecoin', 'xUSD', 18, ''),
     (0x23001f892c0c82b79303edc9b9033cd190bb21c7, 'Crypto-backed stablecoin', 'LUSD', 18, ''),
     (0x750e4c4984a9e0f12978ea6742bc1c5d248f40ed, 'Crypto-backed stablecoin', 'axlUSDC', 6, ''),
     (0x2893Ef551B6dD69F661Ac00F11D93E5Dc5Dc0e99, 'Fiat-backed stablecoin', 'BUIDL', 6, ''),
     (0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a, 'Fiat-backed stablecoin', 'AUSD', 6, ''),
     (0x5c067c80c00ecd2345b05e83a3e758ef799c40b5, 'Fiat-backed stablecoin', 'BRL1', 18, ''),
     (0xe6a537a407488807f0bbeb0038b79004f19dddfb, 'Fiat-backed stablecoin', 'BRLA', 18, ''),
     (0x4ed141110f6eeeaba9a1df36d8c26f684d2475dc, 'Fiat-backed stablecoin', 'BRZ', 18, ''),
     (0x4d1137c03262bcd286f26782033b60af792cd59d, 'Fiat-backed stablecoin', 'CLPC', 18, ''),
     (0x52828daa48c1a9a06f37500882b42daf0be04c3b, 'Fiat-backed stablecoin', 'cNGN', 18, ''),
     (0x12050c705152931cfee3dd56c52fb09dea816c23, 'Fiat-backed stablecoin', 'COPM', 18, ''),
     (0xc2ff25dd99e467d2589b2c26edd270f220f14e47, 'Fiat-backed stablecoin', 'dEURO', 18, ''),
     (0x888883b5f5d21fb10dfeb70e8f9722b9fb0e5e51, 'Fiat-backed stablecoin', 'EUROP', 18, ''),
     (0x4933a85b5b5466fbaf179f72d3de273c287ec2c2, 'Fiat-backed stablecoin', 'EURAU', 18, ''),
     (0xe0b52e49357fd4daf2c15e02058dce6bc0057db4, 'Crypto-backed stablecoin', 'agEUR', 18, ''),
     (0xe0aea583266584dafbb3f9c3211d5588c73fea8d, 'Fiat-backed stablecoin', 'EURe', 18, ''),
     (0xe111178a87a3bff0c8d18decba5798827539ae99, 'Fiat-backed stablecoin', 'EURS', 18, ''),
     (0x554cd6bdd03214b10aafa3e0d4d42de0c5d2937b, 'Fiat-backed stablecoin', 'IDRT', 18, ''),
     (0x649a2da7b28e0d54c13d5eff95d3a660652742cc, 'Fiat-backed stablecoin', 'IDRX', 18, ''),
     (0xdc3326e71d45186f113a2f448984ca0e8d201995, 'Fiat-backed stablecoin', 'XSGD', 6, ''),
     (0xe4095d9372e68d108225c306a4491cacfb33b097, 'Fiat-backed stablecoin', 'VEUR', 18, ''),
     (0x9de41aff9f55219d5bf4359f167d1d0c772a396d, 'Fiat-backed stablecoin', 'CADC', 18, ''),
     (0x2c826035c1c36986117a0e949bd6ad4bab54afe2, 'Fiat-backed stablecoin', 'XIDR', 18, ''),
     (0xe7c3d8c9a439fede00d2600032d5db0be71c3c29, 'Fiat-backed stablecoin', 'JPYC', 18, ''),
     (0x30de46509dbc3a491128f97be0aaf70dc7ff33cb, 'Fiat-backed stablecoin', 'xZAR', 18, ''),
     (0xb755506531786c8ac63b756bab1ac387bacb0c04, 'Fiat-backed stablecoin', 'ZARP', 18, '')

) as temp_table (contract_address, backing, symbol, decimals, name)
