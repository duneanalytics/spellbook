{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds_oracle_addresses'),
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set _1inch_usd = '1INCH / USD' %}
{% set ada_usd = 'ADA / USD' %}
{% set ape_usd = 'APE / USD' %}
{% set arb_usd = 'ARB / USD' %}
{% set atom_usd = 'ATOM / USD' %}
{% set aud_usd = 'AUD / USD' %}
{% set avax_usd = 'AVAX / USD' %}
{% set axs_usd = 'AXS / USD' %}
{% set bal_usd = 'BAL / USD' %}
{% set bnb_usd = 'BNB / USD' %}
{% set brl_usd = 'BRL / USD' %}
{% set btc_eth = 'BTC / ETH' %}
{% set btc_usd_total_marketcap = 'BTC-USD Total Marketcap' %}
{% set busd_usd = 'BUSD / USD' %}
{% set cad_usd = 'CAD / USD' %}
{% set cake_usd = 'CAKE / USD' %}
{% set cbeth_eth = 'CBETH / ETH' %}
{% set chf_usd = 'CHF / USD' %}
{% set cny_usd = 'CNY / USD' %}
{% set comp_usd = 'COMP / USD' %}
{% set crv_usd = 'CRV / USD' %}
{% set cv_index = 'CV Index' %}
{% set cvx_usd = 'CVX / USD' %}
{% set dodo_usd = 'DODO / USD' %}
{% set doge_usd = 'DOGE / USD' %}
{% set dot_usd = 'DOT / USD' %}
{% set dpx_usd = 'DPX / USD' %}
{% set eth_usd_total_marketcap = 'ETH-USD Total Marketcap' %}
{% set eur_usd = 'EUR / USD' %}
{% set frax_usd = 'FRAX / USD' %}
{% set ftm_usd = 'FTM / USD' %}
{% set fxs_usd = 'FXS / USD' %}
{% set gbp_usd = 'GBP / USD' %}
{% set gmx_usd = 'GMX / USD' %}
{% set joe_usd = 'JOE / USD' %}
{% set jpy_usd = 'JPY / USD' %}
{% set knc_usd = 'KNC / USD' %}
{% set krw_usd = 'KRW / USD' %}
{% set l2_sequencer_uptime_status_feed = 'L2 Sequencer Uptime Status Feed' %}
{% set link_eth = 'LINK / ETH' %}
{% set lusd_usd = 'LUSD / USD' %}
{% set magic_usd = 'MAGIC / USD' %}
{% set matic_usd = 'MATIC / USD' %}
{% set mim_usd = 'MIM / USD' %}
{% set mimatic_usd = 'MIMATIC / USD' %}
{% set mkr_usd = 'MKR / USD' %}
{% set near_usd = 'NEAR / USD' %}
{% set nft_blue_chip_total_market_cap_usd = 'NFT Blue Chip Total Market Cap-USD' %}
{% set ohm_index = 'OHM Index' %}
{% set ohmv2_usd = 'OHMv2 / USD' %}
{% set op_usd = 'OP / USD' %}
{% set paxg_usd = 'PAXG / USD' %}
{% set pepe_usd = 'PEPE / USD' %}
{% set php_usd = 'PHP / USD' %}
{% set rdnt_usd = 'RDNT / USD' %}
{% set rpl_usd = 'RPL / USD' %}
{% set sek_usd = 'SEK / USD' %}
{% set sgd_usd = 'SGD / USD' %}
{% set snx_usd = 'SNX / USD' %}
{% set sol_usd = 'SOL / USD' %}
{% set spell_usd = 'SPELL / USD' %}
{% set steth_eth = 'STETH / ETH' %}
{% set steth_usd = 'STETH / USD' %}
{% set sushi_usd = 'SUSHI / USD' %}
{% set stafi_staked_eth_reth_eth_exchange_rate = 'StaFi Staked ETH rETH-ETH Exchange Rate' %}
{% set try_usd = 'TRY / USD' %}
{% set tusd_usd = 'TUSD / USD' %}
{% set total_marketcap_usd = 'Total Marketcap USD' %}
{% set uni_usd = 'UNI / USD' %}
{% set usdd_usd = 'USDD / USD' %}
{% set wbtc_btc = 'WBTC / BTC' %}
{% set wbtc_usd = 'WBTC / USD' %}
{% set wsteth_eth = 'WSTETH / ETH' %}
{% set wti_usd = 'WTI / USD' %}
{% set xag_usd = 'XAG / USD' %}
{% set xau_usd = 'XAU / USD' %}
{% set xrp_usd = 'XRP / USD' %}
{% set yfi_usd = 'YFI / USD' %}
{% set cbeth_eth_exchange_rate_ = 'cbETH-ETH Exchange Rate ' %}
{% set ibbtc_pricepershare = 'ibBTC PricePerShare' %}
{% set reth_eth_exchange_rate = 'rETH-ETH Exchange Rate' %}
{% set wsteth_steth_exchange_rate = 'wstETH-stETH Exchange Rate' %}

SELECT
   'arbitrum' as blockchain,
   feed_name,
   CAST(decimals AS BIGINT) as decimals,
   proxy_address,
   aggregator_address
FROM (values
  ('{{_1inch_usd}}', 8, 0x4bC735Ef24bf286983024CAd5D03f0738865Aaef, 0xa64344ec6b4971d1FBDaf5550001ac5751EEd599),
  ('{{ada_usd}}', 8, 0xD9f615A9b820225edbA2d821c4A696a0924051c6, 0xeFBC6F8C9806B066fa0Da149349450Be60e5e136),
  ('{{ape_usd}}', 8, 0x221912ce795669f628c51c69b7d0873eDA9C03bB, 0x076577765a3e66db410eCc1372d0B0dB503A42C5),
  ('{{arb_usd}}', 8, 0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6, 0x46de66F10343b59BAcc37dF9b3F67cD0CcC121A3),
  ('{{atom_usd}}', 8, 0xCDA67618e51762235eacA373894F0C79256768fa, 0x77b911DBe147ED5b4554997860D6362A5652fC91),
  ('{{aud_usd}}', 8, 0x9854e9a850e7C354c1de177eA953a6b1fba8Fc22, 0x4258e5D50d737CBBEA347f0115Ad166e234902D7),
  ('{{avax_usd}}', 8, 0x8bf61728eeDCE2F32c456454d87B5d6eD6150208, 0xcf17b68a40f10d3DcEedd9a092F1Df331cE3D9da),
  ('{{axs_usd}}', 8, 0x5B58aA6E0651Ae311864876A55411F481aD86080, 0xA303a72d334e589122454e8e849E147BAd309E73),
  ('{{bal_usd}}', 8, 0xBE5eA816870D11239c543F84b71439511D70B94f, 0x53368bC6a7eB4f4AF3d6974520FEba0295A5daAb),
  ('{{bnb_usd}}', 8, 0x6970460aabF80C5BE983C6b74e5D06dEDCA95D4A, 0x9c65Bc9C18f754129bA00c5298B539e69A32102d),
  ('{{brl_usd}}', 8, 0x04b7384473A2aDF1903E3a98aCAc5D62ba8C2702, 0x5d750CC68ff61E2D68930003f77241f7346ADc84),
  ('{{btc_eth}}', 18, 0xc5a90A6d7e4Af242dA238FFe279e9f2BA0c64B2e, 0x3c8F2d5af2e0F5Ef7C23A08DF6Ad168ece071D4b),
  ('{{btc_usd_total_marketcap}}', 8, 0x7519bCA20e21725557Bb98d9032124f8885a26C2, 0x815d5838677F0f063f7589C1Da44f76241FD0C65),
  ('{{busd_usd}}', 8, 0x8FCb0F3715A82D83270777b3a5f3a7CF95Ce8Eec, 0x6c77960BEB512D955cCe2d5eaA1Ea20A388Ba9a2),
  ('{{cad_usd}}', 8, 0xf6DA27749484843c4F02f5Ad1378ceE723dD61d4, 0x52716c109696C0229E18fDeadFf6f54B3b73784F),
  ('{{cake_usd}}', 8, 0x256654437f1ADA8057684b18d742eFD14034C400, 0x496000e12F6d5A2eC4512a6bE34Fe36ba84E6349),
  ('{{cbeth_eth}}', 18, 0xa668682974E3f121185a3cD94f00322beC674275, 0xbfc294070e8A7594cEAa6C564883E1F9222BC09b),
  ('{{chf_usd}}', 8, 0xe32AccC8c4eC03F6E75bd3621BfC9Fbb234E1FC3, 0xF56e6cb49304c2AD4a7C416665b55a1424014B1F),
  ('{{cny_usd}}', 8, 0xcC3370Bde6AFE51e1205a5038947b9836371eCCb, 0x1b9749e06817433a34D7efAaa0a7f6a94a41E432),
  ('{{comp_usd}}', 8, 0xe7C53FFd03Eb6ceF7d208bC4C13446c76d1E5884, 0x52df0481c6D2Ad7E50889AFd03C8ddd8413ac63d),
  ('{{crv_usd}}', 8, 0xaebDA2c976cfd1eE1977Eac079B4382acb849325, 0x79DaA21a44D1415306Ec17C361e0090bdD4cFCbe),
  ('{{cv_index}}', 18, 0xbcD8bEA7831f392bb019ef3a672CC15866004536, 0xb58AFa4be9B13D896E81D5355C961D2c33172099),
  ('{{cvx_usd}}', 8, 0x851175a919f36c8e30197c09a9A49dA932c2CC00, 0x3d62E33E97de1F0ce913dB62d5972722C2A7E4f6),
  ('{{dodo_usd}}', 8, 0xA33a06c119EC08F92735F9ccA37e07Af08C4f281, 0xc195bA27455182e3Bb6F86dAB5838901604Ba72c),
  ('{{dodo_usd}}', 8, 0xA33a06c119EC08F92735F9ccA37e07Af08C4f281, 0xB61D6E5eCB4188248702F65D59728F607F9E3d2F),
  ('{{doge_usd}}', 8, 0x9A7FB1b3950837a8D9b40517626E11D4127C098C, 0xbF1CD5Cb759f8E21c98A4367B665F43D607E8885),
  ('{{dot_usd}}', 8, 0xa6bC5bAF2000424e90434bA7104ee399dEe80DEc, 0x2F45a77c5024eB546E9E7F445f266c0D3e71e616),
  ('{{dpx_usd}}', 8, 0xc373B9DB0707fD451Bc56bA5E9b029ba26629DF0, 0x2489462e64Ea205386b7b8737609B3701047a77d),
  ('{{eth_usd_total_marketcap}}', 8, 0xB1f70A229FE7cceD0428245db8B1f6C48c7Ea82a, 0xC38d4423efaD7D673b0cD47656ed33F8c15c6a57),
  ('{{eur_usd}}', 8, 0xA14d53bC1F1c0F31B4aA3BD109344E5009051a84, 0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871),
  ('{{frax_usd}}', 8, 0x0809E3d38d1B4214958faf06D8b1B1a2b73f2ab8, 0x5D041081725468Aa43e72ff0445Fde2Ad1aDE775),
  ('{{ftm_usd}}', 8, 0xFeaC1A3936514746e70170c0f539e70b23d36F19, 0x70001968d6ee8f909dE9e730E5b4e586565bbbF9),
  ('{{fxs_usd}}', 8, 0x36a121448D74Fa81450c992A1a44B9b7377CD3a5, 0xf8C6DE435CF8d06897a4A66b21df623D06d2A761),
  ('{{gbp_usd}}', 8, 0x9C4424Fd84C6661F97D8d6b3fc3C1aAc2BeDd137, 0x78f28D363533695458696b42577D2e1728cEa3D1),
  ('{{gmx_usd}}', 8, 0xDB98056FecFff59D032aB628337A4887110df3dB, 0xF6328F007A2FDc547875e24A3BC7e0603fd01727),
  ('{{joe_usd}}', 8, 0x04180965a782E487d0632013ABa488A472243542, 0xa44BCD128E99188565c4849cdfFEea9C773f74ec),
  ('{{jpy_usd}}', 8, 0x3dD6e51CB9caE717d5a8778CF79A04029f9cFDF8, 0xadBf1c8a244d537C343d771e2Fa897F3654a1Ae4),
  ('{{knc_usd}}', 8, 0xbF539d4c2106dd4D9AB6D56aed3d9023529Db145, 0x20870D99455B6F9d7c0E6f2608245719d789ff53),
  ('{{krw_usd}}', 8, 0x85bb02E0Ae286600d1c68Bb6Ce22Cc998d411916, 0x244ea8646Cc2342497dFD5D8f841f79e66e598cF),
  ('{{l2_sequencer_uptime_status_feed}}', 0, 0xFdB631F5EE196F0ed6FAa767959853A9F217697D, 0xC1303BBBaf172C55848D3Cb91606d8E27FF38428),
  ('{{link_eth}}', 18, 0xb7c8Fb1dB45007F98A68Da0588e1AA524C317f27, 0xa136978a2c8a92ec5EacC5179642AA2E1c1Eae18),
  ('{{lusd_usd}}', 8, 0x0411D28c94d85A36bC72Cb0f875dfA8371D8fFfF, 0x20CD97619A51d1a6f1910ce62d98Aceb9a13d5e6),
  ('{{magic_usd}}', 8, 0x47E55cCec6582838E173f252D08Afd8116c2202d, 0x5ab0B1e2604d4B708721bc3cD1ce962958b4297E),
  ('{{matic_usd}}', 8, 0x52099D4523531f678Dfc568a7B1e5038aadcE1d6, 0xA4A2b2000d447CC1086d15C077730008b0251FFD),
  ('{{mim_usd}}', 8, 0x87121F6c9A9F6E90E59591E4Cf4804873f54A95b, 0x0Ae17556F9698fC47C365A746AB9CddCB17F3809),
  ('{{mimatic_usd}}', 8, 0x59644ec622243878d1464A9504F9e9a31294128a, 0xc339c4c7c58cb1d964b7B66f846572D5C355441a),
  ('{{mkr_usd}}', 8, 0xdE9f0894670c4EFcacF370426F10C3AD2Cdf147e, 0x55EfafFC2764389a08FFDFcd36AEe2C30610d42c),
  ('{{near_usd}}', 8, 0xBF5C3fB2633e924598A46B9D07a174a9DBcF57C0, 0xbA4492A430fE9bEF7Abbd9C78b953A6E1aA48CFA),
  ('{{nft_blue_chip_total_market_cap_usd}}', 8, 0x8D0e319eBAA8DF32e088e469062F85abF2eBe599, 0x1A8220ac22762F08bE1cD17eE3b6FFfFe96c921c),
  ('{{ohm_index}}', 9, 0x48C4721354A3B29D80EF03C65E6644A37338a0B1, 0x1a2B9E570fe9032562F0E200D03cd29DCf082268),
  ('{{ohmv2_usd}}', 8, 0x761aaeBf021F19F198D325D7979965D0c7C9e53b, 0x1Fa1c3c6371a723a0773421E20ea86Bb02A637Ae),
  ('{{ohmv2_usd}}', 8, 0x761aaeBf021F19F198D325D7979965D0c7C9e53b, 0x09410414Ca067b8763ce62DBEcA8160be9cfD548),
  ('{{op_usd}}', 8, 0x205aaD468a11fd5D34fA7211bC6Bad5b3deB9b98, 0x0526ED34229425a10888C6972906CF2a820D6d13),
  ('{{paxg_usd}}', 8, 0x2BA975D4D7922cD264267Af16F3bD177F206FE3c, 0x2e4c363449E2EC7E93cd9ed4F3843c2CA4497108),
  ('{{pepe_usd}}', 18, 0x02DEd5a7EDDA750E3Eb240b54437a54d57b74dBE, 0x67db13c76Ce77E8FEef9B500616162eC142597ac),
  ('{{pepe_usd}}', 18, 0x02DEd5a7EDDA750E3Eb240b54437a54d57b74dBE, 0x2147745C6c7164E3124B4cC24cb903F1c0dfd47f),
  ('{{php_usd}}', 8, 0xfF82AAF635645fD0bcc7b619C3F28004cDb58574, 0x5E4C65194F6F33a8BF7E9B95F1D0Ca9d611F6D62),
  ('{{rdnt_usd}}', 8, 0x20d0Fcab0ECFD078B036b6CAf1FaC69A6453b352, 0x94cD888Bea0dE39DD0b41396a9311c5212635EB7),
  ('{{rpl_usd}}', 8, 0xF0b7159BbFc341Cc41E7Cb182216F62c6d40533D, 0x44D3ab6c4b98A3F9f241E1753b2475ad7e502051),
  ('{{sek_usd}}', 8, 0xdE89a55d04DEd40A410877ab87d4F567ee66a1f0, 0xd0DA1FE3A35359564CB7561F6a6AA69A1eCEc8d5),
  ('{{sgd_usd}}', 8, 0xF0d38324d1F86a176aC727A4b0c43c9F9d9c5EB1, 0x7DD7029668c78af259a27A7696d468a152F06E53),
  ('{{snx_usd}}', 8, 0x054296f0D036b95531B4E14aFB578B80CFb41252, 0x382f3C547e3EBd1D08cd0CAf6e5F0a7b0B350A11),
  ('{{sol_usd}}', 8, 0x24ceA4b8ce57cdA5058b924B9B9987992450590c, 0x8C4308F7cbD7fB829645853cD188500D7dA8610a),
  ('{{spell_usd}}', 8, 0x383b3624478124697BEF675F07cA37570b73992f, 0xf6bACC7750c23A34b996A355A6E78b17Fc4BaEdC),
  ('{{spell_usd}}', 8, 0x383b3624478124697BEF675F07cA37570b73992f, 0x4B3F43e086790a71270750192472fB61dF7E8566),
  ('{{steth_eth}}', 18, 0xded2c52b75B24732e9107377B7Ba93eC1fFa4BAf, 0xBFf434a1B44677D4ce302da64d84bB4d305c0D49),
  ('{{steth_usd}}', 8, 0x07C5b924399cc23c24a95c8743DE4006a32b7f2a, 0xE5B5Be82015444c04B281CF4aFa6A99130ED83a2),
  ('{{sushi_usd}}', 8, 0xb2A8BA74cbca38508BA1632761b56C897060147C, 0xe4A492420eBdA03B04973Ed1E46d5fe9F3b077EF),
  ('{{sushi_usd}}', 8, 0xb2A8BA74cbca38508BA1632761b56C897060147C, 0x0D02B6EbA98BdA364953D9b1DEf8eedC19fFd516),
  ('{{stafi_staked_eth_reth_eth_exchange_rate}}', 18, 0x052d4200b624b07262F574af26C71A6553996Ab5, 0x73BBf768a429a4f80c47D0d22DdBCe5823c97d7a),
  ('{{try_usd}}', 8, 0xE8f8AfE4b56c6C421F691bfAc225cE61b2C7CD05, 0x8548A931B2E5605a8936089B75F0bF66136e2393),
  ('{{tusd_usd}}', 8, 0x6fAbee62266Da6686EE2744C6f15bb8352d2f28D, 0xEC2E9000B487F28Fd03455f9277bE3c96a3180b2),
  ('{{total_marketcap_usd}}', 8, 0x4763b84cdBc5211B9e0a57D5E39af3B3b2440012, 0x7B9845A634822c543F5Ce544Dd7D7797B79a06b8),
  ('{{uni_usd}}', 8, 0x9C917083fDb403ab5ADbEC26Ee294f6EcAda2720, 0xeFc5061B7a8AeF31F789F1bA5b3b8256674F2B71),
  ('{{usdd_usd}}', 8, 0x4Ee1f9ec1048979930aC832a3C1d18a0b4955a02, 0xd9fCb26FE3D4589c3e2ecD6A2A3af54EdDB67240),
  ('{{wbtc_btc}}', 8, 0x0017abAc5b6f291F9164e35B1234CA1D697f9CF4, 0x1Cde96670e1e779b13dDfd6a5c6D19349cc6a642),
  ('{{wbtc_usd}}', 8, 0xd0C7101eACbB49F3deCcCc166d238410D6D46d57, 0xb20bd22d3D2E5a628523d37b3DED569598EB649b),
  ('{{wsteth_eth}}', 18, 0xb523AE262D20A936BC152e6023996e46FDC2A95D, 0x0e9b5c79e005a30bf3fbB4d8ccCB6B0082ac5a17),
  ('{{wti_usd}}', 8, 0x594b919AD828e693B935705c3F816221729E7AE8, 0x4B552F6496a9E5E05B51BDdC372E623b76560155),
  ('{{xag_usd}}', 8, 0xC56765f04B248394CF1619D20dB8082Edbfa75b1, 0x0A4d55347817738166e2eF8302e12F99CfbDdEDD),
  ('{{xau_usd}}', 8, 0x1F954Dc24a49708C26E0C1777f16750B5C6d5a2c, 0xd35B0e1Fd468Bd264570C64f28Ea48F778bc0DfC),
  ('{{xrp_usd}}', 8, 0xB4AD57B52aB9141de9926a3e0C8dc6264c2ef205, 0x1Fe010E64b5Df97BCB034870334Ff8FbB02ad019),
  ('{{yfi_usd}}', 8, 0x745Ab5b69E01E2BE1104Ca84937Bb71f96f5fB21, 0x660e7aF290F540205A84dccC1F40D0269fC936F5),
  ('{{cbeth_eth_exchange_rate_}}', 18, 0x0518673439245BB95A58688Bc31cd513F3D5bDd6, 0x7D8DCd217E30c5232Aa1E50bA0E4c56DdB5E387C),
  ('{{ibbtc_pricepershare}}', 18, 0x80dd57c45B73f3c70feAF1BFe1bcdF384703E558, 0x519936385413Dbce03da2A4d34125a555D3f0438),
  ('{{reth_eth_exchange_rate}}', 18, 0xF3272CAfe65b190e76caAF483db13424a3e23dD2, 0x0AAAFE4278AA9D4514552f2743cBFa5a5Bdce04b),
  ('{{wsteth_steth_exchange_rate}}', 18, 0xB1552C5e96B312d0Bf8b554186F846C40614a540, 0xb39bfAD6295724E01E079Ee3aa78a378Eff6dEB0)
) a (feed_name, decimals, proxy_address, aggregator_address)
