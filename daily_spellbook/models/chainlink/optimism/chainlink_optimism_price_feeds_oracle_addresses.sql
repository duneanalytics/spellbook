{{
  config(
    alias='price_feeds_oracle_addresses',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll","linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set aave_usd = 'AAVE / USD' %}
{% set ada_usd = 'ADA / USD' %}
{% set ankr_usd = 'ANKR / USD' %}
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
{% set bonk_usd = 'BONK / USD' %}
{% set brl_usd = 'BRL / USD' %}
{% set btc_usd = 'BTC / USD' %}
{% set cbeth_eth = 'CBETH / ETH' %}
{% set crv_usd = 'CRV / USD' %}
{% set cvx_usd = 'CVX / USD' %}
{% set dai_usd = 'DAI / USD' %}
{% set doge_usd = 'DOGE / USD' %}
{% set dot_usd = 'DOT / USD' %}
{% set dydx_usd = 'DYDX / USD' %}
{% set eth_usd = 'ETH / USD' %}
{% set ethx_eth = 'ETHx / ETH' %}
{% set eur_usd = 'EUR / USD' %}
{% set fet_usd = 'FET / USD' %}
{% set fil_usd = 'FIL / USD' %}
{% set floki_usd = 'FLOKI / USD' %}
{% set flow_usd = 'FLOW / USD' %}
{% set frax_usd = 'FRAX / USD' %}
{% set ftm_usd = 'FTM / USD' %}
{% set fxs_usd = 'FXS / USD' %}
{% set gbp_usd = 'GBP / USD' %}
{% set gmx_usd = 'GMX / USD' %}
{% set grt_usd = 'GRT / USD' %}
{% set imx_usd = 'IMX / USD' %}
{% set inj_usd = 'INJ / USD' %}
{% set inr_usd = 'INR / USD' %}
{% set jpy_usd = 'JPY / USD' %}
{% set jto_usd = 'JTO / USD' %}
{% set jup_usd = 'JUP / USD' %}
{% set knc_usd = 'KNC / USD' %}
{% set l2_sequencer_uptime_status_feed = 'L2 Sequencer Uptime Status Feed' %}
{% set ldo_usd = 'LDO / USD' %}
{% set link_eth = 'LINK / ETH' %}
{% set link_usd = 'LINK / USD' %}
{% set ltc_usd = 'LTC / USD' %}
{% set lusd_usd = 'LUSD / USD' %}
{% set matic_usd = 'MATIC / USD' %}
{% set meme_usd = 'MEME / USD' %}
{% set mimatic_usd = 'MIMATIC / USD' %}
{% set near_usd = 'NEAR / USD' %}
{% set one_usd = 'ONE / USD' %}
{% set op_usd = 'OP / USD' %}
{% set ordi_usd = 'ORDI / USD' %}
{% set pendle_usd = 'PENDLE / USD' %}
{% set pepe_usd = 'PEPE / USD' %}
{% set perp_usd = 'PERP / USD' %}
{% set pyth_usd = 'PYTH / USD' %}
{% set reth_eth = 'RETH / ETH' %}
{% set rseth_eth = 'RSETH / ETH' %}
{% set rune_usd = 'RUNE / USD' %}
{% set sand_usd = 'SAND / USD' %}
{% set shib_usd = 'SHIB / USD' %}
{% set snx_usd = 'SNX / USD' %}
{% set sol_usd = 'SOL / USD' %}
{% set steth_usd = 'STETH / USD' %}
{% set strk_usd = 'STRK / USD' %}
{% set stx_usd = 'STX / USD' %}
{% set sui_usd = 'SUI / USD' %}
{% set susd_usd = 'SUSD / USD' %}
{% set synthetix_aggregator_debt_ratio = 'Synthetix Aggregator Debt Ratio' %}
{% set synthetix_aggregator_issued_synths = 'Synthetix Aggregator Issued Synths' %}
{% set tbtc_usd = 'TBTC / USD' %}
{% set tia_usd = 'TIA / USD' %}
{% set trx_usd = 'TRX / USD' %}
{% set total_marketcap_usd = 'Total Marketcap USD' %}
{% set uni_usd = 'UNI / USD' %}
{% set usdc_usd = 'USDC / USD' %}
{% set usdt_usd = 'USDT / USD' %}
{% set usde_usd = 'USDe / USD' %}
{% set velo_usd = 'VELO / USD' %}
{% set waves_usd = 'WAVES / USD' %}
{% set wbtc_usd = 'WBTC / USD' %}
{% set wsteth_eth = 'WSTETH / ETH' %}
{% set wsteth_usd = 'WSTETH / USD' %}
{% set xag_usd = 'XAG / USD' %}
{% set xau_usd = 'XAU / USD' %}
{% set xmr_usd = 'XMR / USD' %}
{% set xrp_usd = 'XRP / USD' %}
{% set zil_usd = 'ZIL / USD' %}
{% set sfrax_frax_exchange_rate = 'sFRAX / FRAX Exchange Rate' %}
{% set susde_usde_exchange_rate = 'sUSDe / USDe Exchange Rate' %}
{% set woeth_oeth_exchange_rate = 'wOETH / OETH Exchange Rate' %}
{% set weeth_eth = 'weETH / ETH' %}
{% set weeth_eeth_exchange_rate = 'weETH / eETH Exchange Rate' %}
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
  ('{{ankr_usd}}', 8, 0xaE2f8ca8d89c3E4521B918D9D5F5bB30e937d68a, 0x4420e05bA6826150fa8D325700be8A3B6B8E3d27),
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
  ('{{bonk_usd}}', 18, 0xec236454209A76a6deCdf5C1183aE2Eb5e82a829, 0xDaD904E1C8387E0626De1443F112C9d0123e5a03),
  ('{{brl_usd}}', 8, 0xB22900D4D0CEa5DB0B3bb08565a9f0f4a831D32C, 0xCa80A73840718826a7A8b6b7216Bd5fDa12c121A),
  ('{{btc_usd}}', 8, 0xD702DD976Fb76Fffc2D3963D037dfDae5b04E593, 0x0C1272d2aC652D10d03bb4dEB0D31F15ea3EAb2b),
  ('{{cbeth_eth}}', 18, 0x138b809B8472fF09Cd3E075E6EcbB2e42D41d870, 0x647dA0ebfD5314aF3Bd53eA40541c6B67169e6D0),
  ('{{crv_usd}}', 8, 0xbD92C6c284271c227a1e0bF1786F468b539f51D9, 0x7c56d3650f9aCD992b3Aa635C04A311c54Ad264c),
  ('{{cvx_usd}}', 8, 0x955b05dD4573dDFAfB47cb78db16B1Fa127E6e71, 0xc51C6af1B2c0184F101D9d23d059bDaD2fd330aF),
  ('{{dai_usd}}', 8, 0x8dBa75e83DA73cc766A7e5a0ee71F656BAb470d6, 0xbCe7579e241e5d676c2371Dc21891489dAcDA250),
  ('{{doge_usd}}', 8, 0xC6066533917f034Cf610c08e1fe5e9c7eADe0f54, 0x8AfC1cC622Be1Cd1644579c9c7eC3fbbA6bD02d2),
  ('{{dot_usd}}', 8, 0x28e67BAeEB5dE7A788f3Dde6CF6ee491369Bb3Fa, 0xF030366b47eB1a9b14AD570381d29647E40955Af),
  ('{{dydx_usd}}', 8, 0xee35A95c9a064491531493D8b380bC40A4CCd0Da, 0x19BcA7C81f3ed561a49326b78468EaC64D0E8F2a),
  ('{{eth_usd}}', 8, 0x13e3Ee699D1909E989722E753853AE30b17e08c5, 0x02f5E9e9dcc66ba6392f6904D5Fcf8625d9B19C9),
  ('{{ethx_eth}}', 18, 0x4Fe3caF5752AD3EEE3BfC0Bb4D07069E569bc66C, 0x411b984Affa2241e2a404E9e7716107f8a7B7432),
  ('{{eur_usd}}', 8, 0x3626369857A10CcC6cc3A6e4f5C2f5984a519F20, 0xAA75acE4575AbBE1D237D991a7461f497a56a8F0),
  ('{{fet_usd}}', 8, 0xf37c76163b2918bB4533579D449524F8542E64AD, 0xA2f199CDb286C716b4315Ee216192d8BDE617611),
  ('{{fil_usd}}', 8, 0x66F61FEe824c1dF059BccCC5F21ca39e083EefDf, 0xB42F9F7c0F9997f62AE521CEF14B20a58bd9E088),
  ('{{floki_usd}}', 8, 0x34E0E85CeEc6be6146c4f0115769a29a9539222e, 0x3E313e778dA811F79A66570b8703c66204112Dfd),
  ('{{flow_usd}}', 8, 0x2fF1EB7D0ceC35959F0248E9354c3248c6683D9b, 0x0542BbBfbc26A86EA92d2b7f6Da07DAf0CA091eD),
  ('{{frax_usd}}', 8, 0xc7D132BeCAbE7Dcc4204841F33bae45841e41D9C, 0xaB544FDAD5F68f0F8e53284f942D76177635A3D6),
  ('{{ftm_usd}}', 8, 0xc19d58652d6BfC6Db6FB3691eDA6Aa7f3379E4E9, 0x13f11f2139C10A48eCD7A6A14d804f90b2cFC89A),
  ('{{fxs_usd}}', 8, 0xB9B16330671067B1b062B9aC2eFd2dB75F03436E, 0xc2212835DE6cb9Ef5e26b04E64f7798472Ff2812),
  ('{{gbp_usd}}', 8, 0x540D48C01F946e729174517E013Ad0bdaE5F08C0, 0x7FeD35C1e4C303F272E4fcdF19865E85DfA44f72),
  ('{{gmx_usd}}', 8, 0x62f42f70ba85De1086476bB6BADE926d0E0b8a4C, 0x0eDD9aC354033Ef766eCD45cb235d81139Df3d90),
  ('{{grt_usd}}', 8, 0xfa042d5F474d7A39454C594CCfE014Ea011495f2, 0x444fd822CbCfDC0F24c78f7DD71F67785CddeA43),
  ('{{imx_usd}}', 8, 0x26Fce884555FAe5F0E4701cc976FE8D8bB111A38, 0x5D860ee3A9F47dfd86d40aec1EF7DDD876356F71),
  ('{{inj_usd}}', 8, 0x90CC16F5493894eff84a5Fedd1dcE297d174fEEf, 0x73316EF731f3C7219482716682C063dBbd1602F2),
  ('{{inr_usd}}', 8, 0x5535e67d8f99c8ebe961E1Fc1F6DDAE96FEC82C9, 0x28a6B219403c1Dac04172cBb8cC1aB8bF5925830),
  ('{{jpy_usd}}', 8, 0x536944c3A71FEb7c1E5C66Ee37d1a148d8D8f619, 0xaE4c8567C942B974Af4A860380c99A8D03C6148E),
  ('{{jto_usd}}', 8, 0xFC3b7bd4368b2919f67E437f8c6Ca42C7FD55dd5, 0xa44681BdaE78DB54cAc3d7f862d6A5BaE8c79CbC),
  ('{{jup_usd}}', 8, 0x5eb9F7baCd59C886fBD9aa2C0a891223482a1ed4, 0x57640993dbB9C8DAF6269f94f27F11586385AD65),
  ('{{knc_usd}}', 8, 0xCB24d22aF35986aC1feb8874AdBbDF68f6dC2e96, 0xe4391393205B6265585250E7A026103a0D1E168d),
  ('{{l2_sequencer_uptime_status_feed}}', 0, 0x371EAD81c9102C9BF4874A9075FFFf170F2Ee389, 0x58218ea7422255EBE94e56b504035a784b7AA204),
  ('{{ldo_usd}}', 8, 0x221618871470f78D8a3391d35B77dFb3C0fbc383, 0xB6b7B9b2362F87F204f1CCadDD8832D3a0557dce),
  ('{{link_eth}}', 18, 0x464A1515ADc20de946f8d0DEB99cead8CEAE310d, 0xE67a10DA53Fcd59fae7e47F155c290cb5Defb4B0),
  ('{{link_usd}}', 8, 0xCc232dcFAAE6354cE191Bd574108c1aD03f86450, 0x5d101824C693C70a68FFc3CDb0Cc394F3a4fb9Ec),
  ('{{ltc_usd}}', 8, 0x45954efBD01f5A12428A09E4C38b8434C3dD4Ac3, 0xfC7608cf76F489191Cb319DD6167aEEE387Bb251),
  ('{{lusd_usd}}', 8, 0x9dfc79Aaeb5bb0f96C6e9402671981CdFc424052, 0x19dC743a5E9a73eefAbA7047C7CEeBc650F37336),
  ('{{matic_usd}}', 8, 0x0ded608AFc23724f614B76955bbd9dFe7dDdc828, 0x1C1df24f0d06415fc3F58b1c1fDadd5fC85d2950),
  ('{{meme_usd}}', 8, 0xC6884869673a6960486FE0f6B0E775A53521e433, 0xeBEE769fF6A85E3Ba27384C5BA0d1fcdf5f2176c),
  ('{{mimatic_usd}}', 8, 0x73A3919a69eFCd5b19df8348c6740bB1446F5ed0, 0x1C1245eEfB57d50F90EFc4070b508f4f24c3aB7A),
  ('{{near_usd}}', 8, 0xca6fa4b8CB365C02cd3Ba70544EFffe78f63ac82, 0xf9eCc598293bd5Fb4F700AEB5C4Fb63B23CFE8Aa),
  ('{{one_usd}}', 8, 0x7CFB4fac1a2FDB1267F8bc17FADc12804AC13CFE, 0x663ed3D2aB9F8d5282a94BA4636E346e59bDdEB9),
  ('{{op_usd}}', 8, 0x0D276FC14719f9292D5C1eA2198673d1f4269246, 0x4F6dFDFd4d68F68b2692E79f9e94796fC8015770),
  ('{{ordi_usd}}', 8, 0x30795BeACc0f43920EF1288dB6676B5e205AE288, 0x28FE62353a3461511b3De51b922b86c7d4cEA2e0),
  ('{{pendle_usd}}', 8, 0x58F23F80bF389DB1af9e3aA8c59679806749A8a4, 0xADe1f55d13D3B8eA4a6605B58Dc38372d6af6132),
  ('{{pepe_usd}}', 18, 0x64Ecf089a6594Be781908D5a26FC8fA6CB08A2C7, 0x903F58eE6d6c3c2Ca26427c8F917F6aE515827B1),
  ('{{perp_usd}}', 8, 0xA12CDDd8e986AF9288ab31E58C60e65F2987fB13, 0xE18a4E99F019F92CD72E0C7C05599d76a89296Cd),
  ('{{pyth_usd}}', 8, 0x0838cFe6A97C9CE1611a6Ed17252477a3c71eBEb, 0xE4003Ce4321FBd0a0b691690f917e3A82B97628c),
  ('{{reth_eth}}', 18, 0xb429DE60943a8e6DeD356dca2F93Cd31201D9ed0, 0x823DAdDA66b631776660B849E734B95A4F3241Bf),
  ('{{rseth_eth}}', 18, 0x03fe94a215E3842deD931769F913d93FF33d0051, 0xBaA412C256a3C021B6B3D5acD28113019F9AC41d),
  ('{{rune_usd}}', 8, 0x372cc5e685115A56F14fa7e4716F1294e04c278A, 0x1aafcf49E103a71b31506Cb05FB072ED1B5b0414),
  ('{{sand_usd}}', 8, 0xAE33e077a02071E62d342E449Afd9895b016d541, 0x5d1345669278128B77AF9662C5D6B5e0b2FFD54A),
  ('{{shib_usd}}', 8, 0xd1e56e7657C0E0d20c0e11C2B6ae0D90932d5665, 0xB6B77f1696bc4F95860228286d27f7f4df5D26e4),
  ('{{snx_usd}}', 8, 0x2FCF37343e916eAEd1f1DdaaF84458a359b53877, 0x584F57911b6EEDab5503E202f8e193663c9bd3DB),
  ('{{sol_usd}}', 8, 0xC663315f7aF904fbbB0F785c32046dFA03e85270, 0x92C9B9C512759f5D04563eFa3698FC4fbF735d59),
  ('{{steth_usd}}', 8, 0x41878779a388585509657CE5Fb95a80050502186, 0x12922291D1FcD0d121B5C88f061047fE18732743),
  ('{{strk_usd}}', 8, 0x8814dEC83E2862A3792A0D6aDFC48CF76Add1890, 0xba4a83372c28E4Ef0925d174f9FEf55743B87A4a),
  ('{{stx_usd}}', 8, 0x602eB777BE29Fbe63349A33306bD73Ff333D02bB, 0x19e0f61d3e8C3501b665Eb4a88Ceac5CFCE38293),
  ('{{sui_usd}}', 8, 0xEaf1a9fe242aa9928faedc6CE7e09aD4875f7133, 0xe05a8c52B2e813c9605CFB8F073178ebe5A74705),
  ('{{susd_usd}}', 8, 0x7f99817d87baD03ea21E05112Ca799d715730efe, 0x17D582034c038BaEb17A9E2A969d06f550d3586b),
  ('{{synthetix_aggregator_debt_ratio}}', 27, 0x94A178f2c480D14F8CdDa908D173d7a73F779cb7, 0x0D5642c6329adB3246c13D78B429a9FB1965a0d8),
  ('{{synthetix_aggregator_issued_synths}}', 18, 0x37AAFb2EE35F1250A001202C660B13c301D2130b, 0x22f04BC4162D63730dCde051FDFD97B4f55fF63B),
  ('{{tbtc_usd}}', 8, 0x5a61374950D4BFa5a3D4f2CA36FC1d23A92b6f21, 0x057B4ffE41aFb2104C3355a8396bab7c64E4017F),
  ('{{tia_usd}}', 8, 0xD7bC56BBF8D555936cb5121f38d1d362c586776A, 0x5d782463840e3a2Ed55e425916d498319f289DEd),
  ('{{trx_usd}}', 8, 0x0E09921cf7801A5aD47B892C8727593275625a9f, 0x206A01D5b59B7D7315B6bC7B5866f62A6fdFF7bA),
  ('{{total_marketcap_usd}}', 8, 0x15772F61e4cDC81c7C1c6c454724CE9c7065A6fF, 0x530Ab34385CA1d134fFd33D267f5A2788d645039),
  ('{{uni_usd}}', 8, 0x11429eE838cC01071402f21C219870cbAc0a59A0, 0x85A48ded8c35d82f8f29844e25dD51a70a23c93d),
  ('{{usdc_usd}}', 8, 0x16a9FA2FDa030272Ce99B29CF780dFA30361E0f3, 0xd1Cb03cc31caa72D34dba7eBE21897D9580c4AF0),
  ('{{usdt_usd}}', 8, 0xECef79E109e997bCA29c1c0897ec9d7b03647F5E, 0xAc37790fF4aBf9483fAe2D1f62fC61fE6b8E4789),
  ('{{usde_usd}}', 8, 0xEEDF0B095B5dfe75F3881Cb26c19DA209A27463a, 0x6f0D003A0743F5acea5680B1D128bd433B07ffCE),
  ('{{velo_usd}}', 8, 0x0f2Ed59657e391746C1a097BDa98F2aBb94b1120, 0x381FA26795F866c7FE760C0cca682f0f2563ad56),
  ('{{waves_usd}}', 8, 0x776003ECdF644F87a95B05da549b5e646d5F2Ae4, 0x503465204d3e093146B1E8762e2b221240E0eDA7),
  ('{{wbtc_usd}}', 8, 0x718A5788b89454aAE3A028AE9c111A29Be6c2a6F, 0x65F2c716937FB44b2C28417A7AfC2DACcF1C2D61),
  ('{{wsteth_eth}}', 18, 0x524299Ab0987a7c4B3c8022a35669DdcdC715a10, 0x034f1d70092e81b7738459f02F409a5c5c4b8189),
  ('{{wsteth_usd}}', 8, 0x698B585CbC4407e2D54aa898B2600B53C68958f7, 0x0d110cC7876d73c3C4190324bCF4C59416bBD259),
  ('{{xag_usd}}', 8, 0x290dd71254874f0d4356443607cb8234958DEe49, 0xcC341634464b6FD1221e4d517cD7801155ABaC55),
  ('{{xau_usd}}', 8, 0x8F7bFb42Bf7421c2b34AAD619be4654bFa7B3B8B, 0x78F049f6da1aC1dcA50D6D8f184Acf47eB269852),
  ('{{xmr_usd}}', 8, 0x2a8D91686A048E98e6CCF1A89E82f40D14312672, 0xDa6fCf88c718eCEB18c2c08A543562b1146F4996),
  ('{{xrp_usd}}', 8, 0x8788F0DBDa7678244Ac7FF09d963d7696D56A8a0, 0xb14cbe04a49bF352B939576f9f9665E1D8DC02d2),
  ('{{zil_usd}}', 8, 0x1520874FC216f5F07E03607303Df2Fda6C3Fc203, 0x397c2082dA7A0962A4FBF14e62E72dbCefB7a7Dc),
  ('{{sfrax_frax_exchange_rate}}', 18, 0x8f096bFFe37313Ad6bD5B9fF48F9FF6E4E5Cd065, 0xf15d466a604932d222020196156C9021B13a3F5d),
  ('{{susde_usde_exchange_rate}}', 18, 0x85342bC62aadef58f029ab6d17D643949e6F073e, 0xd57a242FB40ED4526083B6fA05238B3d57f78D45),
  ('{{woeth_oeth_exchange_rate}}', 18, 0x70843CE8E54d2b87Ee02B1911c06EA5632cd07d3, 0x4a7eb4e962D8d3eA1D6074A12c5a581f2d616481),
  ('{{weeth_eth}}', 18, 0xb4479d436DDa5c1A79bD88D282725615202406E3, 0x818E89b7FC0dF4683a4D3768c4fDf2612A73277A),
  ('{{weeth_eeth_exchange_rate}}', 18, 0x72EC6bF88effEd88290C66DCF1bE2321d80502f5, 0x9269B5F560bDEdb4b34EAEE607b89fAA44A7f20B),
  ('{{wsteth_steth_exchange_rate}}', 18, 0xe59EBa0D492cA53C6f46015EEa00517F2707dc77, 0x6E7A3ceB4797D0Fd7b9854B251929ad68849951a)
) a (feed_name, decimals, proxy_address, aggregator_address)
