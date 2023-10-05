{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds_oracle_addresses'),
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll","linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set aave_usd = 'AAVE / USD' %}
{% set ada_usd = 'ADA / USD' %}
{% set ape_usd = 'APE / USD' %}
{% set apt_usd = 'APT / USD' %}
{% set arb_usd = 'ARB / USD' %}
{% set atom_usd = 'ATOM / USD' %}
{% set aud_usd = 'AUD / USD' %}
{% set avax_usd = 'AVAX / USD' %}
{% set axs_usd = 'AXS / USD' %}
{% set bal_usd = 'BAL / USD' %}
{% set bch_usd = 'BCH / USD' %}
{% set blur_usd = 'BLUR / USD' %}
{% set bnb_usd = 'BNB / USD' %}
{% set bond_usd = 'BOND / USD' %}
{% set brl_usd = 'BRL / USD' %}
{% set btc_usd = 'BTC / USD' %}
{% set busd_usd = 'BUSD / USD' %}
{% set cad_usd = 'CAD / USD' %}
{% set cbeth_eth = 'CBETH / ETH' %}
{% set comp_usd = 'COMP / USD' %}
{% set crv_usd = 'CRV / USD' %}
{% set dai_usd = 'DAI / USD' %}
{% set doge_usd = 'DOGE / USD' %}
{% set dot_usd = 'DOT / USD' %}
{% set dydx_usd = 'DYDX / USD' %}
{% set etc_usd = 'ETC / USD' %}
{% set eth_btc = 'ETH / BTC' %}
{% set eth_usd = 'ETH / USD' %}
{% set eur_usd = 'EUR / USD' %}
{% set fil_usd = 'FIL / USD' %}
{% set floki_usd = 'FLOKI / USD' %}
{% set flow_usd = 'FLOW / USD' %}
{% set frax_usd = 'FRAX / USD' %}
{% set ftm_usd = 'FTM / USD' %}
{% set fxs_usd = 'FXS / USD' %}
{% set gbp_usd = 'GBP / USD' %}
{% set gmx_usd = 'GMX / USD' %}
{% set imx_usd = 'IMX / USD' %}
{% set inj_usd = 'INJ / USD' %}
{% set inr_usd = 'INR / USD' %}
{% set jpy_usd = 'JPY / USD' %}
{% set knc_usd = 'KNC / USD' %}
{% set l2_sequencer_uptime_status_feed = 'L2 Sequencer Uptime Status Feed' %}
{% set ldo_usd = 'LDO / USD' %}
{% set link_eth = 'LINK / ETH' %}
{% set link_usd = 'LINK / USD' %}
{% set ltc_usd = 'LTC / USD' %}
{% set lusd_usd = 'LUSD / USD' %}
{% set matic_usd = 'MATIC / USD' %}
{% set mav_usd = 'MAV / USD' %}
{% set mimatic_usd = 'MIMATIC / USD' %}
{% set mkr_usd = 'MKR / USD' %}
{% set near_usd = 'NEAR / USD' %}
{% set one_usd = 'ONE / USD' %}
{% set op_usd = 'OP / USD' %}
{% set pepe_usd = 'PEPE / USD' %}
{% set perp_usd = 'PERP / USD' %}
{% set rndr_usd = 'RNDR / USD' %}
{% set rpl_usd = 'RPL / USD' %}
{% set rune_usd = 'RUNE / USD' %}
{% set sand_usd = 'SAND / USD' %}
{% set shib_usd = 'SHIB / USD' %}
{% set snx_usd = 'SNX / USD' %}
{% set sol_usd = 'SOL / USD' %}
{% set steth_usd = 'STETH / USD' %}
{% set sui_usd = 'SUI / USD' %}
{% set susd_usd = 'SUSD / USD' %}
{% set synthetix_aggregator_debt_ratio = 'Synthetix Aggregator Debt Ratio' %}
{% set synthetix_aggregator_issued_synths = 'Synthetix Aggregator Issued Synths' %}
{% set trx_usd = 'TRX / USD' %}
{% set total_marketcap_usd = 'Total Marketcap USD' %}
{% set uni_usd = 'UNI / USD' %}
{% set usdc_usd = 'USDC / USD' %}
{% set usdt_usd = 'USDT / USD' %}
{% set waves_usd = 'WAVES / USD' %}
{% set wbtc_usd = 'WBTC / USD' %}
{% set wld_usd = 'WLD / USD' %}
{% set wsteth_eth = 'WSTETH / ETH' %}
{% set wsteth_usd = 'WSTETH / USD' %}
{% set xag_usd = 'XAG / USD' %}
{% set xau_usd = 'XAU / USD' %}
{% set xmr_usd = 'XMR / USD' %}
{% set xrp_usd = 'XRP / USD' %}
{% set yfi_usd = 'YFI / USD' %}
{% set zil_usd = 'ZIL / USD' %}
{% set reth_eth_exchange_rate = 'rETH-ETH Exchange Rate' %}
{% set wsteth_steth_exchange_rate = 'wstETH-stETH Exchange Rate' %}

SELECT
   'optimism' as blockchain,
   feed_name,
   CAST(decimals AS BIGINT) as decimals,
   proxy_address,
   aggregator_address
FROM (values
  ('{{aave_usd}}', 8, 0x338ed6787f463394D24813b297401B9F05a8C9d1, 0x81cC0c227BF9bFB8088b14755DfcA65f7892203b),
  ('{{ada_usd}}', 8, 0x43dEa17DeE1ca50c6266acb59b32659E44D3ee5D, 0xC2262cA31b16AE1Cfe6F7612f49E79B821e31162),
  ('{{ape_usd}}', 8, 0x89178957E9bD07934d7792fFc0CF39f11c8C2B1F, 0x2Dd486F1FA76Fd1228a9c818C552c6A92F138453),
  ('{{apt_usd}}', 8, 0x48f2EcF0Bd180239AEF474a9da945F2e2d41daA3, 0x3442bB3aD11957449D9Af5aCE9d17709240dcCe7),
  ('{{arb_usd}}', 8, 0x8f14546d0B960793180ee355B73fA55041a4a356, 0x7E36B8C677D84556B4742F9d7791019bC7d408Db),
  ('{{atom_usd}}', 8, 0xEF89db2eA46B4aD4E333466B6A486b809e613F39, 0x81d9a9056e9Af6585010B784Df5853a0fDEf8b11),
  ('{{aud_usd}}', 8, 0x39be70E93D2D285C9E71be7f70FC5a45A7777B14, 0x6AA7cC6feA036Bd57f36E2b82878c15012c48771),
  ('{{avax_usd}}', 8, 0x5087Dc69Fd3907a016BD42B38022F7f024140727, 0xa6d25eEBae9c841C44AD01c9176556a4c2189961),
  ('{{axs_usd}}', 8, 0x805a61D54bb686e57F02D1EC96A1491C7aF40893, 0x7A18889f137B593f4E03C0A698A4360f43d1731c),
  ('{{bal_usd}}', 8, 0x30D9d31C1ac29Bc2c2c312c1bCa9F8b3D60e2376, 0x44f690526B76D91072fb0427B0A24b882E612455),
  ('{{bch_usd}}', 8, 0x33E047119359161288bcB143e0C15467C7151d4c, 0x9c41500de0162Cc0bC4798982C867860C1601a60),
  ('{{blur_usd}}', 8, 0x517C2557c29F7c53Aa5F97a1DAE465E0d5C174AA, 0xb785e9aa87cDB29cC11a3f2b8bd17E6279275A35),
  ('{{bnb_usd}}', 8, 0xD38579f7cBD14c22cF1997575eA8eF7bfe62ca2c, 0x25dD1949cDb81f5fc38841a8abF342c4EF48dbfd),
  ('{{bond_usd}}', 8, 0x8fCfb87fc17CfD5775d234AcFd1753764899Bf20, 0x3b06B9b3ead7Ec34AE67E2D7f73B128dA09C583a),
  ('{{brl_usd}}', 8, 0xB22900D4D0CEa5DB0B3bb08565a9f0f4a831D32C, 0xCa80A73840718826a7A8b6b7216Bd5fDa12c121A),
  ('{{btc_usd}}', 8, 0xD702DD976Fb76Fffc2D3963D037dfDae5b04E593, 0x0C1272d2aC652D10d03bb4dEB0D31F15ea3EAb2b),
  ('{{busd_usd}}', 8, 0xC1cB3b7cbB3e786aB85ea28489f332f4FAEd5Bc4, 0xD24E1CdD2F9c0A070F73081B5f79BdD0d42EFA2f),
  ('{{cad_usd}}', 8, 0x6fD5e4a193459FC7DFCFc674357a123F655f6EF8, 0x615209a932768861908161cCCEfcCac9b582ACe8),
  ('{{cbeth_eth}}', 18, 0x138b809B8472fF09Cd3E075E6EcbB2e42D41d870, 0x647dA0ebfD5314aF3Bd53eA40541c6B67169e6D0),
  ('{{comp_usd}}', 8, 0xe1011160d78a80E2eEBD60C228EEf7af4Dfcd4d7, 0xa7F0FF864196729787Cce72f78D769ecA926bA1D),
  ('{{crv_usd}}', 8, 0xbD92C6c284271c227a1e0bF1786F468b539f51D9, 0x7c56d3650f9aCD992b3Aa635C04A311c54Ad264c),
  ('{{dai_usd}}', 8, 0x8dBa75e83DA73cc766A7e5a0ee71F656BAb470d6, 0xbCe7579e241e5d676c2371Dc21891489dAcDA250),
  ('{{doge_usd}}', 8, 0xC6066533917f034Cf610c08e1fe5e9c7eADe0f54, 0x8AfC1cC622Be1Cd1644579c9c7eC3fbbA6bD02d2),
  ('{{dot_usd}}', 8, 0x28e67BAeEB5dE7A788f3Dde6CF6ee491369Bb3Fa, 0xF030366b47eB1a9b14AD570381d29647E40955Af),
  ('{{dydx_usd}}', 8, 0xee35A95c9a064491531493D8b380bC40A4CCd0Da, 0x19BcA7C81f3ed561a49326b78468EaC64D0E8F2a),
  ('{{etc_usd}}', 8, 0xb7B9A39CC63f856b90B364911CC324dC46aC1770, 0x544A5aBfD49782b68d58E69Bc52204b692A86d9E),
  ('{{eth_btc}}', 8, 0xe4b9bcD7d0AA917f19019165EB89BdbbF36d2cBe, 0x90AC3f96131699b7920004a58717C5Eac8E5c9Cc),
  ('{{eth_usd}}', 8, 0x13e3Ee699D1909E989722E753853AE30b17e08c5, 0x02f5E9e9dcc66ba6392f6904D5Fcf8625d9B19C9),
  ('{{eur_usd}}', 8, 0x3626369857A10CcC6cc3A6e4f5C2f5984a519F20, 0xAA75acE4575AbBE1D237D991a7461f497a56a8F0),
  ('{{fil_usd}}', 8, 0x66F61FEe824c1dF059BccCC5F21ca39e083EefDf, 0xB42F9F7c0F9997f62AE521CEF14B20a58bd9E088),
  ('{{floki_usd}}', 8, 0x34E0E85CeEc6be6146c4f0115769a29a9539222e, 0x3E313e778dA811F79A66570b8703c66204112Dfd),
  ('{{flow_usd}}', 8, 0x2fF1EB7D0ceC35959F0248E9354c3248c6683D9b, 0x0542BbBfbc26A86EA92d2b7f6Da07DAf0CA091eD),
  ('{{frax_usd}}', 8, 0xc7D132BeCAbE7Dcc4204841F33bae45841e41D9C, 0xaB544FDAD5F68f0F8e53284f942D76177635A3D6),
  ('{{ftm_usd}}', 8, 0xc19d58652d6BfC6Db6FB3691eDA6Aa7f3379E4E9, 0x13f11f2139C10A48eCD7A6A14d804f90b2cFC89A),
  ('{{fxs_usd}}', 8, 0xB9B16330671067B1b062B9aC2eFd2dB75F03436E, 0xc2212835DE6cb9Ef5e26b04E64f7798472Ff2812),
  ('{{gbp_usd}}', 8, 0x540D48C01F946e729174517E013Ad0bdaE5F08C0, 0x7FeD35C1e4C303F272E4fcdF19865E85DfA44f72),
  ('{{gmx_usd}}', 8, 0x62f42f70ba85De1086476bB6BADE926d0E0b8a4C, 0x0eDD9aC354033Ef766eCD45cb235d81139Df3d90),
  ('{{imx_usd}}', 8, 0x26Fce884555FAe5F0E4701cc976FE8D8bB111A38, 0x5D860ee3A9F47dfd86d40aec1EF7DDD876356F71),
  ('{{inj_usd}}', 8, 0x90CC16F5493894eff84a5Fedd1dcE297d174fEEf, 0x73316EF731f3C7219482716682C063dBbd1602F2),
  ('{{inr_usd}}', 8, 0x5535e67d8f99c8ebe961E1Fc1F6DDAE96FEC82C9, 0x28a6B219403c1Dac04172cBb8cC1aB8bF5925830),
  ('{{jpy_usd}}', 8, 0x536944c3A71FEb7c1E5C66Ee37d1a148d8D8f619, 0xaE4c8567C942B974Af4A860380c99A8D03C6148E),
  ('{{knc_usd}}', 8, 0xCB24d22aF35986aC1feb8874AdBbDF68f6dC2e96, 0xe4391393205B6265585250E7A026103a0D1E168d),
  ('{{l2_sequencer_uptime_status_feed}}', 0, 0x371EAD81c9102C9BF4874A9075FFFf170F2Ee389, 0x58218ea7422255EBE94e56b504035a784b7AA204),
  ('{{ldo_usd}}', 8, 0x221618871470f78D8a3391d35B77dFb3C0fbc383, 0xB6b7B9b2362F87F204f1CCadDD8832D3a0557dce),
  ('{{link_eth}}', 18, 0x464A1515ADc20de946f8d0DEB99cead8CEAE310d, 0xE67a10DA53Fcd59fae7e47F155c290cb5Defb4B0),
  ('{{link_usd}}', 8, 0xCc232dcFAAE6354cE191Bd574108c1aD03f86450, 0x5d101824C693C70a68FFc3CDb0Cc394F3a4fb9Ec),
  ('{{ltc_usd}}', 8, 0x45954efBD01f5A12428A09E4C38b8434C3dD4Ac3, 0xfC7608cf76F489191Cb319DD6167aEEE387Bb251),
  ('{{lusd_usd}}', 8, 0x9dfc79Aaeb5bb0f96C6e9402671981CdFc424052, 0x19dC743a5E9a73eefAbA7047C7CEeBc650F37336),
  ('{{matic_usd}}', 8, 0x0ded608AFc23724f614B76955bbd9dFe7dDdc828, 0x1C1df24f0d06415fc3F58b1c1fDadd5fC85d2950),
  ('{{mav_usd}}', 8, 0x51E06250C8E46c8E5DE41ac8B917a47D706128C2, 0xE5D13134e228d74eF8b3881618e04221D50543bA),
  ('{{mimatic_usd}}', 8, 0x73A3919a69eFCd5b19df8348c6740bB1446F5ed0, 0x1C1245eEfB57d50F90EFc4070b508f4f24c3aB7A),
  ('{{mkr_usd}}', 8, 0x607b417DF51e0E1ed3A12fDb7FC0e8307ED250F3, 0x46D677b285728Bb641FDa3470873637372a587fd),
  ('{{near_usd}}', 8, 0xca6fa4b8CB365C02cd3Ba70544EFffe78f63ac82, 0xf9eCc598293bd5Fb4F700AEB5C4Fb63B23CFE8Aa),
  ('{{one_usd}}', 8, 0x7CFB4fac1a2FDB1267F8bc17FADc12804AC13CFE, 0x663ed3D2aB9F8d5282a94BA4636E346e59bDdEB9),
  ('{{op_usd}}', 8, 0x0D276FC14719f9292D5C1eA2198673d1f4269246, 0x4F6dFDFd4d68F68b2692E79f9e94796fC8015770),
  ('{{pepe_usd}}', 18, 0x64Ecf089a6594Be781908D5a26FC8fA6CB08A2C7, 0x903F58eE6d6c3c2Ca26427c8F917F6aE515827B1),
  ('{{perp_usd}}', 8, 0xA12CDDd8e986AF9288ab31E58C60e65F2987fB13, 0xE18a4E99F019F92CD72E0C7C05599d76a89296Cd),
  ('{{rndr_usd}}', 8, 0x53623FD50C5Fd8788746af00F088FD7f06fD4116, 0x663D15E1E80E227a146Aa616996A6b8A95bb6822),
  ('{{rpl_usd}}', 8, 0xADE082c91A6AeCC86fC11704a830e933e1b382eA, 0xAd1e27Afb932d835ff9829bD16534E5E2c4A6fEd),
  ('{{rune_usd}}', 8, 0x372cc5e685115A56F14fa7e4716F1294e04c278A, 0x1aafcf49E103a71b31506Cb05FB072ED1B5b0414),
  ('{{sand_usd}}', 8, 0xAE33e077a02071E62d342E449Afd9895b016d541, 0x5d1345669278128B77AF9662C5D6B5e0b2FFD54A),
  ('{{shib_usd}}', 8, 0xd1e56e7657C0E0d20c0e11C2B6ae0D90932d5665, 0xB6B77f1696bc4F95860228286d27f7f4df5D26e4),
  ('{{snx_usd}}', 8, 0x2FCF37343e916eAEd1f1DdaaF84458a359b53877, 0x584F57911b6EEDab5503E202f8e193663c9bd3DB),
  ('{{sol_usd}}', 8, 0xC663315f7aF904fbbB0F785c32046dFA03e85270, 0x92C9B9C512759f5D04563eFa3698FC4fbF735d59),
  ('{{steth_usd}}', 8, 0x41878779a388585509657CE5Fb95a80050502186, 0x12922291D1FcD0d121B5C88f061047fE18732743),
  ('{{sui_usd}}', 8, 0xEaf1a9fe242aa9928faedc6CE7e09aD4875f7133, 0xe05a8c52B2e813c9605CFB8F073178ebe5A74705),
  ('{{susd_usd}}', 8, 0x7f99817d87baD03ea21E05112Ca799d715730efe, 0x17D582034c038BaEb17A9E2A969d06f550d3586b),
  ('{{synthetix_aggregator_debt_ratio}}', 27, 0x94A178f2c480D14F8CdDa908D173d7a73F779cb7, 0x0D5642c6329adB3246c13D78B429a9FB1965a0d8),
  ('{{synthetix_aggregator_issued_synths}}', 18, 0x37AAFb2EE35F1250A001202C660B13c301D2130b, 0x22f04BC4162D63730dCde051FDFD97B4f55fF63B),
  ('{{trx_usd}}', 8, 0x0E09921cf7801A5aD47B892C8727593275625a9f, 0x206A01D5b59B7D7315B6bC7B5866f62A6fdFF7bA),
  ('{{total_marketcap_usd}}', 8, 0x15772F61e4cDC81c7C1c6c454724CE9c7065A6fF, 0x530Ab34385CA1d134fFd33D267f5A2788d645039),
  ('{{uni_usd}}', 8, 0x11429eE838cC01071402f21C219870cbAc0a59A0, 0x85A48ded8c35d82f8f29844e25dD51a70a23c93d),
  ('{{usdc_usd}}', 8, 0x16a9FA2FDa030272Ce99B29CF780dFA30361E0f3, 0xd1Cb03cc31caa72D34dba7eBE21897D9580c4AF0),
  ('{{usdt_usd}}', 8, 0xECef79E109e997bCA29c1c0897ec9d7b03647F5E, 0xAc37790fF4aBf9483fAe2D1f62fC61fE6b8E4789),
  ('{{waves_usd}}', 8, 0x776003ECdF644F87a95B05da549b5e646d5F2Ae4, 0x503465204d3e093146B1E8762e2b221240E0eDA7),
  ('{{wbtc_usd}}', 8, 0x718A5788b89454aAE3A028AE9c111A29Be6c2a6F, 0x65F2c716937FB44b2C28417A7AfC2DACcF1C2D61),
  ('{{wld_usd}}', 8, 0x4e1C6B168DCFD7758bC2Ab9d2865f1895813D236, 0xB001D353633cd96B68000aa915C8A8A136d90A98),
  ('{{wsteth_eth}}', 18, 0x524299Ab0987a7c4B3c8022a35669DdcdC715a10, 0x034f1d70092e81b7738459f02F409a5c5c4b8189),
  ('{{wsteth_usd}}', 8, 0x698B585CbC4407e2D54aa898B2600B53C68958f7, 0x0d110cC7876d73c3C4190324bCF4C59416bBD259),
  ('{{xag_usd}}', 8, 0x290dd71254874f0d4356443607cb8234958DEe49, 0xcC341634464b6FD1221e4d517cD7801155ABaC55),
  ('{{xau_usd}}', 8, 0x8F7bFb42Bf7421c2b34AAD619be4654bFa7B3B8B, 0x78F049f6da1aC1dcA50D6D8f184Acf47eB269852),
  ('{{xmr_usd}}', 8, 0x2a8D91686A048E98e6CCF1A89E82f40D14312672, 0xDa6fCf88c718eCEB18c2c08A543562b1146F4996),
  ('{{xrp_usd}}', 8, 0x8788F0DBDa7678244Ac7FF09d963d7696D56A8a0, 0xb14cbe04a49bF352B939576f9f9665E1D8DC02d2),
  ('{{yfi_usd}}', 8, 0x5cdC797acCBf57EE2363Fed9701262Abc87a232e, 0x6D95344ba8d22a7d1C5BF1822ed80A70f411740a),
  ('{{zil_usd}}', 8, 0x1520874FC216f5F07E03607303Df2Fda6C3Fc203, 0x397c2082dA7A0962A4FBF14e62E72dbCefB7a7Dc),
  ('{{reth_eth_exchange_rate}}', 18, 0x22F3727be377781d1579B7C9222382b21c9d1a8f, 0xA57074acA7FCa1A3Ce8e79ECFE31c2C11bE80982),
  ('{{wsteth_steth_exchange_rate}}', 18, 0xe59EBa0D492cA53C6f46015EEa00517F2707dc77, 0x6E7A3ceB4797D0Fd7b9854B251929ad68849951a)
) a (feed_name, decimals, proxy_address, aggregator_address)
