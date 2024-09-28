{{
  config(
    
    alias='price_feeds_oracle_addresses',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set _1inch_usd = '1INCH / USD' %}
{% set aave_usd = 'AAVE / USD' %}
{% set alpha_usd = 'ALPHA / USD' %}
{% set avax_usd = 'AVAX / USD' %}
{% set bal_usd = 'BAL / USD' %}
{% set bnb_usd = 'BNB / USD' %}
{% set btc_usd = 'BTC / USD' %}
{% set chf_usd = 'CHF / USD' %}
{% set comp_usd = 'COMP / USD' %}
{% set cream_usd = 'CREAM / USD' %}
{% set crv_usd = 'CRV / USD' %}
{% set dai_usd = 'DAI / USD' %}
{% set doge_usd = 'DOGE / USD' %}
{% set dot_usd = 'DOT / USD' %}
{% set dpi_usd = 'DPI / USD' %}
{% set eth_usd = 'ETH / USD' %}
{% set eur_usd = 'EUR / USD' %}
{% set fox_usd = 'FOX / USD' %}
{% set ftt_usd = 'FTT / USD' %}
{% set gno_usd = 'GNO / USD' %}
{% set grt_usd = 'GRT / USD' %}
{% set jpy_usd = 'JPY / USD' %}
{% set link_usd = 'LINK / USD' %}
{% set mkr_usd = 'MKR / USD' %}
{% set mxn_usd = 'MXN / USD' %}
{% set perp_usd = 'PERP / USD' %}
{% set ren_usd = 'REN / USD' %}
{% set snx_usd = 'SNX / USD' %}
{% set sol_usd = 'SOL / USD' %}
{% set steth_usd = 'STETH / USD' %}
{% set sushi_usd = 'SUSHI / USD' %}
{% set uma_usd = 'UMA / USD' %}
{% set uni_usd = 'UNI / USD' %}
{% set usdc_usd = 'USDC / USD' %}
{% set usdt_usd = 'USDT / USD' %}
{% set wbtc_usd = 'WBTC / USD' %}
{% set xau_usd = 'XAU / USD' %}
{% set yfi_usd = 'YFI / USD' %}
{% set zil_usd = 'ZIL / USD' %}
{% set wsteth_eth_exchange_rate = 'wstETH-ETH Exchange Rate' %}

SELECT
   'gnosis' as blockchain,
   feed_name,
   CAST(decimals AS BIGINT) as decimals,
   proxy_address,
   aggregator_address
FROM (values
  ('{{_1inch_usd}}', 8, 0xFDF9EB5fafc11Efa65f6FD144898da39a7920Ae8, 0xf50a71387D9D01ED3873E32f1044497327AF1044),
  ('{{aave_usd}}', 8, 0x2b481Dc923Aa050E009113Dca8dcb0daB4B68cDF, 0xD2CCCa5Bb84CB2F6b72B98Ee82c61F67c82DD40B),
  ('{{alpha_usd}}', 8, 0x7969b8018928F3d9faaE9AC71744ed2C1486536F, 0x200F30e782490976EF16D903fc267Af30Eee5182),
  ('{{alpha_usd}}', 8, 0x7969b8018928F3d9faaE9AC71744ed2C1486536F, 0xf5FD312d0435C24f5c9bB8411bFc3D79c23EB0Bd),
  ('{{avax_usd}}', 8, 0x911e08A32A6b7671A80387F93147Ab29063DE9A2, 0xB840C9dBC0964bcD89d6410f34091b2cb6733ADb),
  ('{{bal_usd}}', 8, 0x1b723C855F7D2c2785F99486973271355e782d77, 0x3F2BA1E94112120d11F1a525913134fBE510bF37),
  ('{{bnb_usd}}', 8, 0x6D42cc26756C34F26BEcDD9b30a279cE9Ea8296E, 0x9Af55762115066D3e99AD4d237586344C4ABEAdD),
  ('{{btc_usd}}', 8, 0x6C1d7e76EF7304a40e8456ce883BC56d3dEA3F7d, 0xaD479D707a8f1fAA346Fe1380f76CF993ae54330),
  ('{{chf_usd}}', 8, 0xFb00261Af80ADb1629D3869E377ae1EEC7bE659F, 0x6E2482E011EC31a1960a938791B6B4Ff5BAa3217),
  ('{{chf_usd}}', 8, 0xFb00261Af80ADb1629D3869E377ae1EEC7bE659F, 0xbe18b8F41760878ba6D3b1E9475c4CcAD3D9aA8f),
  ('{{comp_usd}}', 8, 0xBa95Bc8418Ebcdf8a690924E1d4aD5292139F2EA, 0x7cfEA3C34FBEf2e9A53c188a75494aeDC3A743ff),
  ('{{cream_usd}}', 8, 0x3b681e9BF56eFe4b2a14196826230A5843fFF758, 0x9a8cdEA210623550befE64fAf2c8cE6f35bF9d82),
  ('{{cream_usd}}', 8, 0x3b681e9BF56eFe4b2a14196826230A5843fFF758, 0x991bc14CF115d9b6eA518C412c9f4eA234d70C0a),
  ('{{crv_usd}}', 8, 0xC77B83ac3Dd2a761073bD0f281f7b880B2DDDe18, 0x88C76CccaA469614EAad7CED362050062DC2CCeB),
  ('{{dai_usd}}', 8, 0x678df3415fc31947dA4324eC63212874be5a82f8, 0xb65566283CAcE6b281308308da0f0783a613c416),
  ('{{doge_usd}}', 8, 0x824b4A1A0443609A2ADd94a700b770FA5bE31287, 0x817A6F75fA1840847382ab23203EA38eDB7158f6),
  ('{{dot_usd}}', 8, 0x3c30c5c415B2410326297F0f65f5Cbb32f3aefCc, 0x5128beD0c719537b179aF0aF01386caf9B22Baf1),
  ('{{dpi_usd}}', 8, 0x53B1b13E7a5C0DE9A2BeFa1085Ec364BB27e439f, 0x57E62eB3E84a2D12Cc40ACBa84D10b79dA9E7c2E),
  ('{{eth_usd}}', 8, 0xa767f745331D267c7751297D982b050c93985627, 0x059e7Bd8157e0d302dF3626E162B6C835340b311),
  ('{{eur_usd}}', 8, 0xab70BCB260073d036d1660201e9d5405F5829b7a, 0x759be90a34E426042ed7d17916B78a5cD2567dd1),
  ('{{fox_usd}}', 8, 0x3190f6D277Fea03A082Eba20B136f95f0DCCb3dD, 0x1AF770B72Da45e4278cA190370902aDea889EbA3),
  ('{{ftt_usd}}', 8, 0x0CaE8f5c10931f0Ce87Ed9BbB71391C6E93C2C26, 0x7FeD35C1e4C303F272E4fcdF19865E85DfA44f72),
  ('{{ftt_usd}}', 8, 0x0CaE8f5c10931f0Ce87Ed9BbB71391C6E93C2C26, 0x932ab70A49F0C678c9368040068E98f1a5a6A27a),
  ('{{gno_usd}}', 8, 0x22441d81416430A54336aB28765abd31a792Ad37, 0xcA16Ed36A7d1Ae2DC68873D62bce4f9BdCc2d378),
  ('{{gno_usd}}', 8, 0x22441d81416430A54336aB28765abd31a792Ad37, 0x016a45F646bbd35B61fE7A496a75D9Ea69bD243E),
  ('{{grt_usd}}', 8, 0xeBbd67a84e33791F1bcFde74CDc22a71e332e2F1, 0x18064eA9430Dd74E854162Aff10E34aC6Df3838B),
  ('{{grt_usd}}', 8, 0xeBbd67a84e33791F1bcFde74CDc22a71e332e2F1, 0xCff7b6aaF94513566A5821fF77bcC075F5d2273D),
  ('{{jpy_usd}}', 8, 0x2AfB993C670C01e9dA1550c58e8039C1D8b8A317, 0xa5f05b6C586f21b7E6200b6A6a4ADE55eCAB0103),
  ('{{link_usd}}', 8, 0xed322A5ac55BAE091190dFf9066760b86751947B, 0x813a79EfDfd6a4352b7C583d8d38B2B5d1151d7E),
  ('{{mkr_usd}}', 8, 0x51e4024255d0cBd1F4C79AEe6BDB6565Df2C5d1b, 0x88403402B966Dee25724d24Adf56d398D1d7334e),
  ('{{mxn_usd}}', 8, 0xe9cea51a7b1b9B32E057ff62762a2066dA933cD2, 0xD8aE5c2AEec843531Df1e523D775d870f877103C),
  ('{{perp_usd}}', 8, 0x76e76F7E73F3BD42E3c2b4282B50b36E78130B4A, 0xafb768EF3aa6a5756f9F2d11CBE04C4EF90949bf),
  ('{{ren_usd}}', 8, 0x27d4D36968a2BD1Cc3406D99cB1DF50561dBf2a4, 0x9C1Dc429a8d8F10C8ebA522b608bC27F58d6ABE2),
  ('{{ren_usd}}', 8, 0x27d4D36968a2BD1Cc3406D99cB1DF50561dBf2a4, 0xEF2cD9d9B9e5D8101e36DfF9D602D96e819a2eE8),
  ('{{snx_usd}}', 8, 0x3b84d6e6976D5826500572600eB44f9f1753827b, 0x19e5234E9Aad5fb08f58c3569E8d0858664F0cF3),
  ('{{sol_usd}}', 8, 0xB7B7d008c49295A0FF6Eed6dF4Ad3052Fd39d5e6, 0x295442CF6Ba90E91b8a01A7C2dE9d7DB987439C8),
  ('{{steth_usd}}', 8, 0x229e486Ee0D35b7A9f668d10a1e6029eEE6B77E0, 0xcC5a624A98600564992753DafF5Cdfe7a2e58f67),
  ('{{sushi_usd}}', 8, 0xC0a6Bf8d5D408B091D022C3C0653d4056D4B9c01, 0x337D84cE7Cc5e4Da39dDc0e76698e79Ff3b40217),
  ('{{sushi_usd}}', 8, 0xC0a6Bf8d5D408B091D022C3C0653d4056D4B9c01, 0xd73dCdf9795f04bB91Cfc86D86DB5286B2606A26),
  ('{{uma_usd}}', 8, 0xF826E3ff8c0481D2e58DB9d301936F94Cd4fa916, 0x3448BEB14411C2EcF5e86632dC9ad65849ae2a2E),
  ('{{uni_usd}}', 8, 0xd98735d78266c62277Bb4dBf3e3bCdd3694782F4, 0x29b914B44169B04262Ae11173149d7F1131791bC),
  ('{{usdc_usd}}', 8, 0x26C31ac71010aF62E6B486D1132E266D6298857D, 0x30bA871Ee7a08dBd255CdD8e7e035DAd72014E27),
  ('{{usdt_usd}}', 8, 0x68811D7DF835B1c33e6EEae8E7C141eF48d48cc7, 0xc4D924b6baB6FEc909E482b93847D997463f0c79),
  ('{{wbtc_usd}}', 8, 0x00288135bE38B83249F380e9b6b9a04c90EC39eE, 0x5ED6A59735297Bc5D6CB4942913Ae7098E0cD703),
  ('{{xau_usd}}', 8, 0x4a5AB0F60d12a4420d36D3eD9A1F77d8c47EB94c, 0x53BA6D580a9EAB65899Bbc1157c2062a83fa8D9E),
  ('{{yfi_usd}}', 8, 0x14030d5a0C9e63D9606C6f2c8771Fc95b34b07e0, 0x642736Bb512ac22D37171bf57eda17E48E977be8),
  ('{{zil_usd}}', 8, 0x2997eBa3d9c2447c36107bB0F082b8c33566b49c, 0x736C15dB4e4f694c7c899AC91C05Bd5A795CfDD0),
  ('{{wsteth_eth_exchange_rate}}', 18, 0x0064AC007fF665CF8D0D3Af5E0AD1c26a3f853eA, 0x6dcF8CE1982Fc71E7128407c7c6Ce4B0C1722F55)
) a (feed_name, decimals, proxy_address, aggregator_address)
