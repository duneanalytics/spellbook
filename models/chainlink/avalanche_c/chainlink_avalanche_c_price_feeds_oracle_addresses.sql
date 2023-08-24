{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds_oracle_addresses'),
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set aapl_usd = 'AAPL / USD' %}
{% set aave_usd = 'AAVE / USD' %}
{% set aave_network_emergency_count_avalanche_ = 'AAVE Network Emergency Count (Avalanche)' %}
{% set aave_e_por = 'AAVE.e PoR' %}
{% set alpha_usd = 'ALPHA / USD' %}
{% set ampl_usd = 'AMPL / USD' %}
{% set amzn_usd = 'AMZN / USD' %}
{% set ape_usd = 'APE / USD' %}
{% set avax_usd = 'AVAX / USD' %}
{% set axs_usd = 'AXS / USD' %}
{% set bat_usd = 'BAT / USD' %}
{% set brl_usd = 'BRL / USD' %}
{% set btc_usd = 'BTC / USD' %}
{% set btc_b_por = 'BTC.b PoR' %}
{% set busd_usd = 'BUSD / USD' %}
{% set bridgetower_capital_por = 'Bridgetower Capital PoR' %}
{% set cake_usd = 'CAKE / USD' %}
{% set chf_usd = 'CHF / USD' %}
{% set chz_usd = 'CHZ / USD' %}
{% set comp_usd = 'COMP / USD' %}
{% set crv_usd = 'CRV / USD' %}
{% set cvx_usd = 'CVX / USD' %}
{% set calculated_savax_usd = 'Calculated SAVAX / USD' %}
{% set dai_usd = 'DAI / USD' %}
{% set dai_e_por = 'DAI.e PoR' %}
{% set dot_usd = 'DOT / USD' %}
{% set eth_usd = 'ETH / USD' %}
{% set eur_usd = 'EUR / USD' %}
{% set fil_usd = 'FIL / USD' %}
{% set frax_usd = 'FRAX / USD' %}
{% set ftm_usd = 'FTM / USD' %}
{% set ftt_usd = 'FTT / USD' %}
{% set fxs_usd = 'FXS / USD' %}
{% set gmx_usd = 'GMX / USD' %}
{% set googl_usd = 'GOOGL / USD' %}
{% set joe_usd = 'JOE / USD' %}
{% set jpy_usd = 'JPY / USD' %}
{% set knc_usd = 'KNC / USD' %}
{% set link_avax = 'LINK / AVAX' %}
{% set link_usd = 'LINK / USD' %}
{% set link_e_por = 'LINK.e PoR' %}
{% set mana_usd = 'MANA / USD' %}
{% set matic_usd = 'MATIC / USD' %}
{% set meta_usd = 'META / USD' %}
{% set mim_usd = 'MIM / USD' %}
{% set mimatic_usd = 'MIMATIC / USD' %}
{% set mkr_usd = 'MKR / USD' %}
{% set near_usd = 'NEAR / USD' %}
{% set nflx_usd = 'NFLX / USD' %}
{% set ohm_index = 'OHM Index' %}
{% set ohmv2_usd = 'OHMv2 / USD' %}
{% set qi_usd = 'QI / USD' %}
{% set sand_usd = 'SAND / USD' %}
{% set snx_usd = 'SNX / USD' %}
{% set spell_usd = 'SPELL / USD' %}
{% set sushi_usd = 'SUSHI / USD' %}
{% set try_usd = 'TRY / USD' %}
{% set tsla_usd = 'TSLA / USD' %}
{% set tusd_usd = 'TUSD / USD' %}
{% set tusd_por = 'TUSD PoR' %}
{% set uni_usd = 'UNI / USD' %}
{% set usdc_usd = 'USDC / USD' %}
{% set usdc_e_por = 'USDC.e PoR' %}
{% set usdt_usd = 'USDT / USD' %}
{% set usdt_e_por = 'USDT.e PoR' %}
{% set ust_usd = 'UST / USD' %}
{% set wbtc_usd = 'WBTC / USD' %}
{% set wbtc_e_por = 'WBTC.e PoR' %}
{% set weth_e_por = 'WETH.e PoR' %}
{% set woo_eth = 'WOO / ETH' %}
{% set xau_usd = 'XAU / USD' %}
{% set xava_usd = 'XAVA / USD' %}
{% set yfi_usd = 'YFI / USD' %}
{% set zrx_usd = 'ZRX / USD' %}

SELECT
   'avalanche_c' as blockchain,
   feed_name,
   CAST(decimals AS BIGINT) as decimals,
   proxy_address,
   aggregator_address
FROM (values
  ('{{aapl_usd}}', 18, 0x4E4908dE170506b0795BE21bfb6e012770A635B1, 0x8dEDEb1295e946D23B934c66964d7D402B684450),
  ('{{aave_usd}}', 8, 0x3CA13391E9fb38a75330fb28f8cc2eB3D9ceceED, 0xcb7f6eF54bDc05B704a0aCf604A6A16C53d359e1),
  ('{{aave_network_emergency_count_avalanche_}}', 0, 0x41185495Bc8297a65DC46f94001DC7233775EbEe, 0x6987c6600815bA7421ED30d127c4cb354779AAC0),
  ('{{aave_e_por}}', 18, 0x14C4c668E34c09E1FBA823aD5DB47F60aeBDD4F7, 0x1a53159F3366c9FaED485B9D7D4078CCd1569D22),
  ('{{alpha_usd}}', 8, 0x7B0ca9A6D03FE0467A31Ca850f5bcA51e027B3aF, 0x9C81461B6B821407E0a2968F9CEc23e3C7063F84),
  ('{{ampl_usd}}', 8, 0xcf667FB6Bd30c520A435391c50caDcDe15e5e12f, 0x9e107262620CfC6E0e2445df6C0ca0a9aD9Ba627),
  ('{{amzn_usd}}', 18, 0x108F85023B5b1a06aC85713A94047F365A163de1, 0xD974a467454DC34d7a0942977474767c05aBfC7d),
  ('{{ape_usd}}', 8, 0xF0981a2BDE30cf767080d80b93BeCA6204dCC54A, 0x3caF1C0C388384e87d170c00A781fc767Ac44C61),
  ('{{avax_usd}}', 8, 0x0A77230d17318075983913bC2145DB16C7366156, 0x9450A29eF091B625e976cE66f2A5818e20791999),
  ('{{axs_usd}}', 8, 0x155835C5755205597d62703a5A0b37e57a26Ee5C, 0xB7579A25A3394dEef5edb4b72535bE9c67316a41),
  ('{{bat_usd}}', 8, 0xe89B3CE86D25599D1e615C0f6a353B4572FF868D, 0x553BDc8a55569756Bd4bAB24e545752474A2Cd5a),
  ('{{brl_usd}}', 8, 0x609DDDB75518aa4Af84Ac228b62261aE68E52989, 0x1F847C386D97B2a03625bC449Bc7ded815426000),
  ('{{btc_usd}}', 8, 0x2779D32d5166BAaa2B2b658333bA7e6Ec0C65743, 0x154baB1FC1D87fF641EeD0E9Bc0f8a50D880D2B6),
  ('{{btc_b_por}}', 8, 0x99311B4bf6D8E3D3B4b9fbdD09a1B0F4Ad8e06E9, 0x4CDE5134F7DA75A71E3E46614B8E220d86802AD5),
  ('{{busd_usd}}', 8, 0x827f8a0dC5c943F7524Dda178E2e7F275AAd743f, 0x9Cb8E5EA1404d5012C0fc01B7B927AE6Aa09164f),
  ('{{bridgetower_capital_por}}', 18, 0x503707A5DC130877F118B7abd27c37CBFfC44E71, 0x08591f637cb0dF9BE95A34c5Bf85886830c9798f),
  ('{{cake_usd}}', 8, 0x79bD0EDd79dB586F22fF300B602E85a662fc1208, 0x0aCcDFd55026873CB12F75f66513b42fB4974245),
  ('{{chf_usd}}', 8, 0x3B37950485b450edF90cBB85d0cD27308Af4AB9A, 0x55d0a1b961bB75c010970A380f32A94965c3A7E9),
  ('{{chz_usd}}', 8, 0xC4D7270aCc921DE5A17452437257f075C1298eB3, 0xa6C851d8721Fa322C8f578E132782e3B93D02D70),
  ('{{comp_usd}}', 8, 0x9D6AA0AC8c4818433bEA7a74F49C73B57BcEC4Ec, 0x498A8B3E1B7582Ae3Ca3ae22AC544a02dB86D4c5),
  ('{{crv_usd}}', 8, 0x7CF8A6090A9053B01F3DF4D4e6CfEdd8c90d9027, 0xFf6e2c3C9E9a174824a764dbb8222454f6A3ecb1),
  ('{{cvx_usd}}', 8, 0x52F8026423B5E04FdD9E4b5725B68230b71D019b, 0x3FfF4d373C588f8975f3312E1Ba6F70A39Ce3f94),
  ('{{calculated_savax_usd}}', 18, 0x2854Ca10a54800e15A2a25cFa52567166434Ff0a, 0x2223338267fF42814d53aE1c02979164b0528fA4),
  ('{{dai_usd}}', 8, 0x51D7180edA2260cc4F6e4EebB82FEF5c3c2B8300, 0xCC4633a1a85d553623bAC7945Bd87CFad6E6a8c8),
  ('{{dai_e_por}}', 18, 0x976D7fAc81A49FA71EF20694a3C56B9eFB93c30B, 0x32d4055c609b63375427172db799b9dAfBb6cEEd),
  ('{{dot_usd}}', 8, 0xD73a74314AcCb53b30cAfDA0cb61c9772B57C4a2, 0x372367702b83ff993E954cB3f06b44607c5d3c9C),
  ('{{eth_usd}}', 8, 0x976B3D034E162d8bD72D6b9C989d545b839003b0, 0xEfaa69f461E0aaf0be1798b01371Daf14AC55eA8),
  ('{{eur_usd}}', 8, 0x192f2DBA961Bb0277520C082d6bfa87D5961333E, 0x95Edda00bCE60f99Fb0bE38fE500eBd879AB651a),
  ('{{fil_usd}}', 8, 0x2F194315f122d374a27973e259783d5C864A5bf6, 0x934C2eAE6fF26103EE50020b1F452944097F90C4),
  ('{{frax_usd}}', 8, 0xbBa56eF1565354217a3353a466edB82E8F25b08e, 0x5eDC2538E11b67cf93ED145b04E5E457d9F9Cc0B),
  ('{{ftm_usd}}', 8, 0x2dD517B2f9ba49CedB0573131FD97a5AC19ff648, 0xAec3e48648C46B9eda4C8832E2f0A6B23289915d),
  ('{{ftt_usd}}', 8, 0x4f2EaebDD835ebe9108e718C0b6551E868381a88, 0x971Ff919f91fFd1Faa847e1a773e8a547e3eFc82),
  ('{{ftt_usd}}', 8, 0x4f2EaebDD835ebe9108e718C0b6551E868381a88, 0x318BFa8c1CE223836FDE0Ad60E8A5E04fD0d8924),
  ('{{fxs_usd}}', 8, 0x12Af94c3716bbf339Aa26BfD927DDdE63B27D50C, 0x92398CAF00D65e9A63b5d50D1CBD53223137A400),
  ('{{gmx_usd}}', 8, 0x3F968A21647d7ca81Fb8A5b69c0A452701d5DCe8, 0x3Ec39652e73337350a712Fb418DBb4C2a8247673),
  ('{{googl_usd}}', 18, 0xFf20180F7C97C6030497d1D262d444b25FC5B460, 0x64f7ab24f5E2aF4274F9AD200F8892bb83fB9776),
  ('{{joe_usd}}', 8, 0x02D35d3a8aC3e1626d3eE09A78Dd87286F5E8e3a, 0x15811F91fAb76Bd240CAeC783a32f1BAAE41c513),
  ('{{jpy_usd}}', 8, 0xf8B283aD4d969ECFD70005714DD5910160565b94, 0x8F937bBaA8508268cC2c3d2A54c8d01d30eEd679),
  ('{{knc_usd}}', 8, 0x9df2195dc96e6Ef983B1aAC275649F3f28F82Aa1, 0x5474cFC8E5Fa684728bAABBFC95B161134c32758),
  ('{{link_avax}}', 18, 0x1b8a25F73c9420dD507406C3A3816A276b62f56a, 0x3aadC82b68784b49a9e01C0af4c9221b16282e7e),
  ('{{link_usd}}', 8, 0x49ccd9ca821EfEab2b98c60dC60F518E765EDe9a, 0xA2e5d3254F7d6E8C051Afb7F2aeea0dABf21F750),
  ('{{link_e_por}}', 18, 0x943cEF1B112Ca9FD7EDaDC9A46477d3812a382b6, 0x5006C56a2967eF6c9d856704669d2421D3189Be4),
  ('{{mana_usd}}', 8, 0x774f067083f23cAB866310489419C884a6Dc00a8, 0x7609db691E5Db62651466DCcC87AFa8703758942),
  ('{{matic_usd}}', 8, 0x1db18D41E4AD2403d9f52b5624031a2D9932Fd73, 0x92655bd2627C17D36b35f20dA3F4A4084E0ABd6F),
  ('{{meta_usd}}', 8, 0xEb1f59749ACc2eBCBcad084FBBDe4E00452fE8d0, 0x9e943dd8416adCa7710B976FAC4F70F394151f3b),
  ('{{mim_usd}}', 8, 0x54EdAB30a7134A16a54218AE64C73e1DAf48a8Fb, 0x9D0aabA64B0BA4650419a37D14175dA05471793c),
  ('{{mimatic_usd}}', 8, 0x5D1F504211c17365CA66353442a74D4435A8b778, 0x5aF11EEC59e1BaC3F4e2565621B43Cfbe748e698),
  ('{{mkr_usd}}', 8, 0x3E54eB0475051401D093702A5DB84EFa1Ab77b14, 0xB3752dC7c1D71A1B381925EC5e6bbf2950519Aa2),
  ('{{near_usd}}', 8, 0x7FDE7f51dc2580dd051e17A333E28CDC8176da0A, 0x780dE5B35E13e848abc59FA7F532c35De6B1207f),
  ('{{nflx_usd}}', 18, 0x98df0E27B678FafF4CdE48c03C4790f7e2E0754F, 0xd6d8CAfD8c7842DfB447346957d22D5b7Edd49ed),
  ('{{ohm_index}}', 9, 0xB2B94f103406bD5d04d84a1beBc3E89F05EEDDEa, 0x2E7574C025add37FCE2EB88bB0EF34289f50af91),
  ('{{ohmv2_usd}}', 8, 0x1fA4Fc8E55939fC511d048e1ceCaFB4b2d30f9Eb, 0xa94FbCBE967E25CFB132182dd57fbBbEdE765799),
  ('{{ohmv2_usd}}', 8, 0x1fA4Fc8E55939fC511d048e1ceCaFB4b2d30f9Eb, 0x7c28e5fe04694C2BC2b8664492e6B82dFC4c2Ed3),
  ('{{qi_usd}}', 8, 0x36E039e6391A5E7A7267650979fdf613f659be5D, 0x4bc3BeBb7eB60155f8b38771D9926d9A23dad5B5),
  ('{{qi_usd}}', 8, 0x36E039e6391A5E7A7267650979fdf613f659be5D, 0xB6f7e0129439829a3679BD06102fDCAA41ebeE5e),
  ('{{sand_usd}}', 8, 0x6f2A1D4014FED967172FC7caCf7a6e04Cf02752e, 0xb650e28712E26Dd9A34B084DDC32aaC2Ac348e20),
  ('{{snx_usd}}', 8, 0x01752eAAB988ECb0ceBa2C8FC97c4f1d38Bf246D, 0xF01826625694D04A30285355A5F3aEf567E6F676),
  ('{{spell_usd}}', 8, 0x4F3ddF9378a4865cf4f28BE51E10AECb83B7daeE, 0x0a58227E7D7A8175E4F5f8a0D32968d153B9ce59),
  ('{{spell_usd}}', 8, 0x4F3ddF9378a4865cf4f28BE51E10AECb83B7daeE, 0x40B5DF33f06264F66F3764b139dC9Aab7e0a6170),
  ('{{sushi_usd}}', 8, 0x449A373A090d8A1e5F74c63Ef831Ceff39E94563, 0xdE672241200B9309f86AB79fd082423f32fD8f0D),
  ('{{try_usd}}', 8, 0xA61bF273688Ea095b5e4c11f1AF5E763F7aEEE91, 0xEF320d919F4DF79c6f4206eB89f78A0b8f21F496),
  ('{{tsla_usd}}', 18, 0x9BBBfe5C63bC70349a63105A2312Fc6169B60504, 0x3f9fd480148A2741FF2478Def8353D7A9AE75838),
  ('{{tusd_usd}}', 8, 0x9Cf3Ef104A973b351B2c032AA6793c3A6F76b448, 0x2EBa2C3CDF50f5BC20fc23F533B227dB6b10A725),
  ('{{tusd_por}}', 18, 0x45b73930AE07C902275312c6b5BacE505D4a5883, 0x375B72E40c1A16424EAd361eC308b1cBCf5CB721),
  ('{{uni_usd}}', 8, 0x9a1372f9b1B71B3A5a72E092AE67E172dBd7Daaa, 0xA0326D3AD91D7724380c096AA62Ae1d5A8d260A8),
  ('{{usdc_usd}}', 8, 0xF096872672F44d6EBA71458D74fe67F9a77a23B9, 0xfBd998938f8f7210eEC3D1e12E80A10972F02aEd),
  ('{{usdc_e_por}}', 6, 0x63769951E4cfDbDC653dD9BBde63D2Ce0746e5F2, 0xBe865442c3A0042f6d619027c697CA409513fFb4),
  ('{{usdt_usd}}', 8, 0xEBE676ee90Fe1112671f19b6B7459bC678B67e8a, 0xb8AEB9160385fa2D1B63B5E88351238593ba0127),
  ('{{usdt_e_por}}', 6, 0x94D8c2548018C27F1aa078A23C4158206bE1CC72, 0x615049D0F6441b8bF09876A97A28B7719A951f46),
  ('{{ust_usd}}', 8, 0xf58B78581c480caFf667C63feDd564eCF01Ef86b, 0xa01516869D8325Fd18a77b307cA38Cab1Eb8Fdeb),
  ('{{ust_usd}}', 8, 0xf58B78581c480caFf667C63feDd564eCF01Ef86b, 0x8b6C17529B122fE39E7F561749476cB0efc5AE6D),
  ('{{wbtc_usd}}', 8, 0x86442E3a98558357d46E6182F4b262f76c4fa26F, 0xb50D5dB75a844365995C29B534a31536a4C56513),
  ('{{wbtc_e_por}}', 8, 0xebEfEAA58636DF9B20a4fAd78Fad8759e6A20e87, 0xAfde05b14dd17cA71bc484bcEf565746D7938eFE),
  ('{{weth_e_por}}', 18, 0xDDaf9290D057BfA12d7576e6dADC109421F31948, 0x1d29baE52280D3B49FA31aF7dFdb61f9dC5040Af),
  ('{{woo_eth}}', 18, 0xfAa665F5a0e13beea63b6DfF601DD634959690Df, 0x6339dfD6433C305661B060659922a70fC4eEbAC6),
  ('{{xau_usd}}', 8, 0x1F41EF93dece881Ad0b98082B2d44D3f6F0C515B, 0x8B050c37B0c8De8f91C1BF648c6c0A4E4Ed7C6eC),
  ('{{xava_usd}}', 8, 0x4Cf57DC9028187b9DAaF773c8ecA941036989238, 0x1872758F3635aa3CFA58CA30Bc2Ec84e5A2C493F),
  ('{{yfi_usd}}', 8, 0x28043B1Ebd41860B93EC1F1eC19560760B6dB556, 0x27355dF92298c785440a4D16574DF736Eb0627d0),
  ('{{zrx_usd}}', 8, 0xC07CDf938aa113741fB639bf39699926123fB58B, 0x347F6cdbD9514284b301456956c846b7B21F375B)
) a (feed_name, decimals, proxy_address, aggregator_address)
