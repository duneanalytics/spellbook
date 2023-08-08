{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds_oracle_addresses'),
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set aapl_usd = 'AAPL / USD' %}
{% set aave_usd = 'AAVE / USD' %}
{% set alcx_usd = 'ALCX / USD' %}
{% set ape_eth = 'APE / ETH' %}
{% set ape_usd = 'APE / USD' %}
{% set apy_tvl = 'APY TVL' %}
{% set atom_eth = 'ATOM / ETH' %}
{% set atom_usd = 'ATOM / USD' %}
{% set audio_usd = 'AUDIO / USD' %}
{% set axs_eth = 'AXS / ETH' %}
{% set azuki_floor_price_eth = 'Azuki Floor Price / ETH' %}
{% set bal_usd = 'BAL / USD' %}
{% set band_eth = 'BAND / ETH' %}
{% set bat_eth = 'BAT / ETH' %}
{% set bat_usd = 'BAT / USD' %}
{% set beanz_official_floor_price = 'BEANZ Official Floor Price' %}
{% set bit_usd = 'BIT / USD' %}
{% set bnb_usd = 'BNB / USD' %}
{% set bond_eth = 'BOND / ETH' %}
{% set boring_usd = 'BORING / USD' %}
{% set btc_usd = 'BTC / USD' %}
{% set btc_interest_rate_benchmark_curve_1_day = 'BTC Interest Rate Benchmark Curve 1 Day' %}
{% set btc_interest_rate_benchmark_curve_1_month = 'BTC Interest Rate Benchmark Curve 1 Month' %}
{% set btc_interest_rate_benchmark_curve_1_week = 'BTC Interest Rate Benchmark Curve 1 Week' %}
{% set btc_interest_rate_benchmark_curve_2_month = 'BTC Interest Rate Benchmark Curve 2 Month' %}
{% set btc_interest_rate_benchmark_curve_2_week = 'BTC Interest Rate Benchmark Curve 2 Week' %}
{% set btc_interest_rate_benchmark_curve_3_month = 'BTC Interest Rate Benchmark Curve 3 Month' %}
{% set btc_interest_rate_benchmark_curve_3_week = 'BTC Interest Rate Benchmark Curve 3 Week' %}
{% set btc_interest_rate_benchmark_curve_4_month = 'BTC Interest Rate Benchmark Curve 4 Month' %}
{% set btc_interest_rate_benchmark_curve_5_month = 'BTC Interest Rate Benchmark Curve 5 Month' %}
{% set bored_ape_yacht_club_floor_price_eth = 'Bored Ape Yacht Club Floor Price / ETH' %}
{% set cake_usd = 'CAKE / USD' %}
{% set cbeth_eth = 'CBETH / ETH' %}
{% set cel_eth = 'CEL / ETH' %}
{% set celo_eth = 'CELO / ETH' %}
{% set comp_usd = 'COMP / USD' %}
{% set cream_eth = 'CREAM / ETH' %}
{% set cro_usd = 'CRO / USD' %}
{% set cspr_usd = 'CSPR / USD' %}
{% set cspx_usd = 'CSPX / USD' %}
{% set ctsi_eth = 'CTSI / ETH' %}
{% set cv_index = 'CV / Index' %}
{% set cvx_eth = 'CVX / ETH' %}
{% set cvx_usd = 'CVX / USD' %}
{% set cachegold_por_usd = 'CacheGold PoR USD' %}
{% set calculated_xsushi_eth = 'Calculated XSUSHI / ETH' %}
{% set calculated_xsushi_usd = 'Calculated XSUSHI / USD' %}
{% set clonex_floor_price = 'CloneX Floor Price' %}
{% set consumer_price_index = 'Consumer Price Index' %}
{% set coolcats_floor_price = 'CoolCats Floor Price' %}
{% set cryptopunks_floor_price_eth = 'CryptoPunks Floor Price / ETH' %}
{% set cryptoadz_floor_price = 'Cryptoadz Floor Price' %}
{% set dai_usd = 'DAI / USD' %}
{% set dodo_usd = 'DODO / USD' %}
{% set dpi_eth = 'DPI / ETH' %}
{% set dpi_usd = 'DPI / USD' %}
{% set dydx_usd = 'DYDX / USD' %}
{% set doodles_floor_price = 'Doodles Floor Price' %}
{% set enj_usd = 'ENJ / USD' %}
{% set ens_usd = 'ENS / USD' %}
{% set eos_usd = 'EOS / USD' %}
{% set ern_usd = 'ERN / USD' %}
{% set eth_btc = 'ETH / BTC' %}
{% set eth_usd = 'ETH / USD' %}
{% set eurt_usd = 'EURT / USD' %}
{% set farm_eth = 'FARM / ETH' %}
{% set fei_usd = 'FEI / USD' %}
{% set fil_eth = 'FIL / ETH' %}
{% set flow_usd = 'FLOW / USD' %}
{% set for_usd = 'FOR / USD' %}
{% set forth_usd = 'FORTH / USD' %}
{% set fox_usd = 'FOX / USD' %}
{% set frax_eth = 'FRAX / ETH' %}
{% set fast_gas_gwei = 'Fast Gas / Gwei' %}
{% set fluent_us_por = 'Fluent US+ PoR' %}
{% set gbpt_por = 'GBPT PoR' %}
{% set gho_usd = 'GHO / USD' %}
{% set glm_usd = 'GLM / USD' %}
{% set grt_eth = 'GRT / ETH' %}
{% set gtc_eth = 'GTC / ETH' %}
{% set gusd_eth = 'GUSD / ETH' %}
{% set gusd_usd = 'GUSD / USD' %}
{% set hbtc_por = 'HBTC PoR' %}
{% set high_usd = 'HIGH / USD' %}
{% set ib01_usd = 'IB01 / USD' %}
{% set ibta_usd = 'IBTA / USD' %}
{% set idr_usd = 'IDR / USD' %}
{% set ilv_eth = 'ILV / ETH' %}
{% set imx_usd = 'IMX / USD' %}
{% set inj_usd = 'INJ / USD' %}
{% set iotx_usd = 'IOTX / USD' %}
{% set jpegd_azuki_floor_price_eth = 'JPEGd Azuki Floor Price ETH' %}
{% set jpegd_bored_ape_floor_price_eth = 'JPEGd Bored Ape Floor Price ETH' %}
{% set jpegd_bored_ape_kennel_club_floor_price_eth = 'JPEGd Bored Ape Kennel Club Floor Price ETH' %}
{% set jpegd_chromie_floor_price_eth = 'JPEGd Chromie Floor Price ETH' %}
{% set jpegd_clonex_floor_price_eth = 'JPEGd CloneX Floor Price ETH' %}
{% set jpegd_cryptopunks_floor_price_eth = 'JPEGd Cryptopunks Floor Price ETH' %}
{% set jpegd_doodle_floor_price_eth = 'JPEGd Doodle Floor Price ETH' %}
{% set jpegd_fidenza_floor_price_eth = 'JPEGd Fidenza Floor Price ETH' %}
{% set jpegd_meebits_floor_price_eth = 'JPEGd Meebits Floor Price ETH' %}
{% set jpegd_milady_floor_price_eth = 'JPEGd Milady Floor Price ETH' %}
{% set jpegd_mutant_ape_floor_price_eth = 'JPEGd Mutant Ape Floor Price ETH' %}
{% set jpegd_otherdeed_floor_price_eth = 'JPEGd Otherdeed Floor Price ETH' %}
{% set jpegd_otherside_koda_floor_price_eth = 'JPEGd Otherside Koda Floor Price ETH' %}
{% set jpegd_pudgy_penguins_floor_price_eth = 'JPEGd Pudgy Penguins Floor Price ETH' %}
{% set jpegd_ringers_floor_price_eth = 'JPEGd Ringers Floor Price ETH' %}
{% set krw_usd = 'KRW / USD' %}
{% set ksm_usd = 'KSM / USD' %}
{% set link_usd = 'LINK / USD' %}
{% set lon_eth = 'LON / ETH' %}
{% set lrc_eth = 'LRC / ETH' %}
{% set lusd_usd = 'LUSD / USD' %}
{% set mana_usd = 'MANA / USD' %}
{% set matic_usd = 'MATIC / USD' %}
{% set mayc_floor_price = 'MAYC Floor Price' %}
{% set mim_usd = 'MIM / USD' %}
{% set mkr_usd = 'MKR / USD' %}
{% set msft_usd = 'MSFT / USD' %}
{% set metis_healthcheck = 'Metis Healthcheck' %}
{% set moonbirds_floor_price = 'Moonbirds Floor Price' %}
{% set nexus_weth_reserves = 'Nexus wETH Reserves' %}
{% set ohm_eth = 'OHM / ETH' %}
{% set ohmv2_eth = 'OHMv2 / ETH' %}
{% set optimism_healthcheck = 'Optimism Healthcheck' %}
{% set otherdeed_for_otherside_floor_price = 'Otherdeed for Otherside Floor Price' %}
{% set perp_usd = 'PERP / USD' %}
{% set pha_usd = 'PHA / USD' %}
{% set pudgy_penguins_floor_price = 'Pudgy Penguins Floor Price' %}
{% set rai_usd = 'RAI / USD' %}
{% set rari_eth = 'RARI / ETH' %}
{% set ren_usd = 'REN / USD' %}
{% set rep_eth = 'REP / ETH' %}
{% set req_usd = 'REQ / USD' %}
{% set reth_eth = 'RETH / ETH' %}
{% set rpl_usd = 'RPL / USD' %}
{% set rsr_usd = 'RSR / USD' %}
{% set shib_eth = 'SHIB / ETH' %}
{% set shv_usd = 'SHV / USD' %}
{% set sol_usd = 'SOL / USD' %}
{% set spell_usd = 'SPELL / USD' %}
{% set stbt_por = 'STBT PoR' %}
{% set steth_eth = 'STETH / ETH' %}
{% set steth_usd = 'STETH / USD' %}
{% set stg_usd = 'STG / USD' %}
{% set sushi_usd = 'SUSHI / USD' %}
{% set swell_eth_por = 'Swell ETH PoR' %}
{% set synthetix_aggregator_debt_ratio = 'Synthetix Aggregator Debt Ratio' %}
{% set synthetix_aggregator_issued_synths = 'Synthetix Aggregator Issued Synths' %}
{% set tusd_usd = 'TUSD / USD' %}
{% set tusd_por = 'TUSD PoR' %}
{% set tusd_reserves = 'TUSD Reserves' %}
{% set total_marketcap_usd = 'Total Marketcap / USD' %}
{% set uma_eth = 'UMA / ETH' %}
{% set uni_usd = 'UNI / USD' %}
{% set usdd_usd = 'USDD / USD' %}
{% set usdp_usd = 'USDP / USD' %}
{% set ust_eth = 'UST / ETH' %}
{% set ust_usd = 'UST / USD' %}
{% set veefriends_floor_price = 'VeeFriends Floor Price' %}
{% set wbtc_btc = 'WBTC / BTC' %}
{% set wbtc_por = 'WBTC PoR' %}
{% set wing_usd = 'WING / USD' %}
{% set world_of_women_floor_price = 'World of Women Floor Price' %}
{% set xcn_usd = 'XCN / USD' %}
{% set yfi_usd = 'YFI / USD' %}
{% set zrx_usd = 'ZRX / USD' %}
{% set efil_por = 'eFIL PoR' %}

SELECT
   'ethereum' as blockchain,
   feed_name,
   CAST(decimals AS BIGINT) as decimals,
   proxy_address,
   aggregator_address
FROM (values
  ('{{aapl_usd}}', 8, 0x139C8512Cde1778e9b9a8e721ce1aEbd4dD43587, 0xc697FCA98f961169B53e80b718155b55fc1a17d5),
  ('{{aave_usd}}', 8, 0x547a514d5e3769680Ce22B2361c10Ea13619e8a9, 0x8116B273cD75d79C382aFacc706659DEd5E0a59d),
  ('{{alcx_usd}}', 8, 0xc355e4C0B3ff4Ed0B49EaACD55FE29B311f42976, 0xb49322fF269d67A5aF0718C19463eC9EE7BF821E),
  ('{{ape_eth}}', 18, 0xc7de7f4d4C9c991fF62a07D18b3E31e349833A18, 0x72002129A3834d63C57d157DDF069deE37b08F24),
  ('{{ape_usd}}', 8, 0xD10aBbC76679a20055E167BB80A24ac851b37056, 0xa99999b1475F24037e8b6947aBBC7710676E77dd),
  ('{{apy_tvl}}', 8, 0xDb299D394817D8e7bBe297E84AFfF7106CF92F5f, 0x953DA51613067981ff15695695994DD8B1310F6d),
  ('{{atom_eth}}', 18, 0x15c8eA24Ba2d36671Fa22aD4Cff0a8eafe144352, 0x2E3Bc7624a3B44954b53e08c436be44f7f16fD00),
  ('{{atom_usd}}', 8, 0xDC4BDB458C6361093069Ca2aD30D74cc152EdC75, 0x736E09DE064A2a461F197643A26bC1ab7Dc4D5D3),
  ('{{audio_usd}}', 8, 0xBf739E677Edf6cF3408857404746cAcfd7120EB2, 0xd9B4Ac554e3eefE84Ae80F5Dee0D45926233160b),
  ('{{axs_eth}}', 18, 0x8B4fC5b68cD50eAc1dD33f695901624a4a1A0A8b, 0x16423B2B6873225e26564b182b3318aFCdBFcade),
  ('{{azuki_floor_price_eth}}', 18, 0xA8B9A447C73191744D5B79BcE864F343455E1150, 0xF0c3668756b9d9590B334768640FC5ACA02aE739),
  ('{{bal_usd}}', 8, 0xdF2917806E30300537aEB49A7663062F4d1F2b5F, 0xbd9350a3a2fd6e3Ad0a053a567f2609a1bf6c505),
  ('{{band_eth}}', 18, 0x0BDb051e10c9718d1C29efbad442E88D38958274, 0xDF9F750A94bF2Faea84Ab783927290FE5e0F7606),
  ('{{bat_eth}}', 18, 0x0d16d4528239e9ee52fa531af613AcdB23D88c94, 0x821f24DAcA9Ad4910c1EdE316D2713fC923Da698),
  ('{{bat_usd}}', 8, 0x9441D7556e7820B5ca42082cfa99487D56AcA958, 0x98E3F1BE8E0609Ac8a7681f23e15B696F8e8204d),
  ('{{beanz_official_floor_price}}', 18, 0xA97477aB5ab6ED2f6A2B5Cbe59D71e88ad334b90, 0x844962E9c0D7033a1EC9d5931bA8DC9dED265a2B),
  ('{{bit_usd}}', 8, 0x7b33EbfA52F215a30FaD5a71b3FeE57a4831f1F0, 0x382db44bCfb92C398b93e5fF6Cc100FC321140c9),
  ('{{bnb_usd}}', 8, 0x14e613AC84a31f709eadbdF89C6CC390fDc9540A, 0xC45eBD0F901bA6B2B8C7e70b717778f055eF5E6D),
  ('{{bond_eth}}', 18, 0xdd22A54e05410D8d1007c38b5c7A3eD74b855281, 0x5667eE03110045510897aDa33DC561cEfCBcC904),
  ('{{boring_usd}}', 8, 0xde9299851FaC41c6AA43Ec96Cd33C28F74837AA9, 0x37674e9881f173D4f5441e6Fc7ed3C6Cf57435ce),
  ('{{btc_usd}}', 8, 0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c, 0xdBe1941BFbe4410D6865b9b7078e0b49af144D2d),
  ('{{btc_interest_rate_benchmark_curve_1_day}}', 8, 0x1d32df460AE5D738A88Faa44DDA2cA3764627461, 0x07e853CB12096d94d1F3325C472de72eC7D32Efa),
  ('{{btc_interest_rate_benchmark_curve_1_month}}', 8, 0x4C01bcfb622F9aF8053E013e18373E50e6e8632E, 0xa3EA205d5Bc7cd3045A373B1127d35bfe9A7a24d),
  ('{{btc_interest_rate_benchmark_curve_1_week}}', 8, 0xAB4D1bc99781A445BC1D065663d9A831f787124e, 0x042F058fE42DDc70Ba8B70264946849FC0BaA4dE),
  ('{{btc_interest_rate_benchmark_curve_2_month}}', 8, 0x96284a8f283aEE951f359f44006C2d1b72e85957, 0xc76781509F9fa4d5ED045077d968cacB11E25fB0),
  ('{{btc_interest_rate_benchmark_curve_2_week}}', 8, 0x5cB4280b9ca8B0363aA4f4ef609dF403e522ea67, 0xb28AbeE6624b82c3c81A2e89E246c32A84110918),
  ('{{btc_interest_rate_benchmark_curve_3_month}}', 8, 0x9320f0429180ce49C6D4d10A4633c8c92cdb53cB, 0xE4D550E7C0b9Cc23d25270194EEbC3A00ec1Ec04),
  ('{{btc_interest_rate_benchmark_curve_3_week}}', 8, 0x37260ecF9Eb9AB1F813909a25CC8ADde99eD00fa, 0xC509a3D253931b59Ddb3206c78A7bdCc53D221D4),
  ('{{btc_interest_rate_benchmark_curve_4_month}}', 8, 0x775a0B15042ceE8925D3D81481c7b94ffBfc24a7, 0xACDA9f64e45a7247718fe7Ee60Ee41f74C17404f),
  ('{{btc_interest_rate_benchmark_curve_5_month}}', 8, 0x05140252A265F2817aF6caF4a7F79a77F87E0a7c, 0x2D9f12a41A3402129d921888B09C08D72e97109d),
  ('{{bored_ape_yacht_club_floor_price_eth}}', 18, 0x352f2Bc3039429fC2fe62004a1575aE74001CfcE, 0x6DBD8100fBbfF754831Aa90A53c466d431651885),
  ('{{cake_usd}}', 8, 0xEb0adf5C06861d6c07174288ce4D0a8128164003, 0x1C026C25271c1bFbA95B65c848F734a23eA62D4e),
  ('{{cbeth_eth}}', 18, 0xF017fcB346A1885194689bA23Eff2fE6fA5C483b, 0xd74FF3f1b565597E59D44320F53a5C5c8BA85f7b),
  ('{{cel_eth}}', 18, 0x75FbD83b4bd51dEe765b2a01e8D3aa1B020F9d33, 0xd0BB178dEdC71470BA68380CBd99c4A963d01507),
  ('{{celo_eth}}', 18, 0x9ae96129ed8FE0C707D6eeBa7b90bB1e139e543e, 0xffDb505cAd574AF3B73e0f4005CcF54a2da100ae),
  ('{{comp_usd}}', 8, 0xdbd020CAeF83eFd542f4De03e3cF0C28A4428bd5, 0x64d2E1F01A19762dDEE27b1062CC092B66Ff9652),
  ('{{cream_eth}}', 18, 0x82597CFE6af8baad7c0d441AA82cbC3b51759607, 0xf8efB55FbF6E7f48637561182dac1Ef09F38d767),
  ('{{cro_usd}}', 8, 0x00Cb80Cf097D9aA9A3779ad8EE7cF98437eaE050, 0xb2aD164c008Da54FCEaC79Ef18C0a8fE2d935865),
  ('{{cspr_usd}}', 8, 0x9e37a8Ee3bFa8eD6783Db031Dc458d200b226074, 0x30F3037f0E13d6bdeD3c5B7809994F47e9656b4a),
  ('{{cspx_usd}}', 8, 0xF4E1B57FB228879D057ac5AE33973e8C53e4A0e0, 0x9aB931c33E0a21689A823d60e625B57EF1faa9C8),
  ('{{ctsi_eth}}', 18, 0x0a1d1b9847d602e789be38B802246161FFA24930, 0x720192921a4b6fb026fA52bf03F991b65b740147),
  ('{{cv_index}}', 18, 0x1B58B67B2b2Df71b4b0fb6691271E83A0fa36aC5, 0xAC28f6D70c6C6d5089e506eFb80624B8ECb666F8),
  ('{{cvx_eth}}', 18, 0xC9CbF687f43176B302F03f5e58470b77D07c61c6, 0xf1F7F7BFCc5E9D6BB8D9617756beC06A5Cbe1a49),
  ('{{cvx_usd}}', 8, 0xd962fC30A72A84cE50161031391756Bf2876Af5D, 0x8d73Ac44Bf11CadCDc050BB2BcCaE8c519555f1a),
  ('{{cachegold_por_usd}}', 8, 0x5586bF404C7A22A4a4077401272cE5945f80189C, 0x6CeA38508B186DE36AAfd0f3B513E708691bc0C4),
  ('{{calculated_xsushi_eth}}', 18, 0xF05D9B6C08757EAcb1fbec18e36A1B7566a13DEB, 0xdEaa4288c85e7e0be40BCE49E76D4e321d20fC36),
  ('{{calculated_xsushi_usd}}', 8, 0xCC1f5d9e6956447630d703C8e93b2345c2DE3D13, 0xAB5041D720ab0CDB3342F5bC7Ac6Cc14B6c70727),
  ('{{clonex_floor_price}}', 18, 0x021264d59DAbD26E7506Ee7278407891Bb8CDCCc, 0xB187B5A5A4B0A2Ae32FaEDf0FE4845203E0B7b11),
  ('{{consumer_price_index}}', 18, 0x9a51192e065ECC6BDEafE5e194ce54702DE4f1f5, 0x5a0efD6D1a058A46D3Ac4511861adB8F3540BD49),
  ('{{coolcats_floor_price}}', 18, 0xF49f8F5b931B0e4B4246E4CcA7cD2083997Aa83d, 0xaBd6dc0E14bdC628E62Cc946897C7fEfDCDdcD10),
  ('{{cryptopunks_floor_price_eth}}', 18, 0x01B6710B01cF3dd8Ae64243097d91aFb03728Fdd, 0xF0c85c0F7dC37e1605a0Db446a2A0e33Df7a3358),
  ('{{cryptoadz_floor_price}}', 18, 0xFaA8F6073845DBe5627dAA3208F78A3043F99bcA, 0xc609c4fADdA31980769c9C6716F438f0a6059547),
  ('{{dai_usd}}', 8, 0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9, 0x478238a1c8B862498c74D0647329Aef9ea6819Ed),
  ('{{dodo_usd}}', 8, 0x9613A51Ad59EE375e6D8fa12eeef0281f1448739, 0x3600713c848dE06c8346900E3deDd3CAc21c06ff),
  ('{{dodo_usd}}', 8, 0x9613A51Ad59EE375e6D8fa12eeef0281f1448739, 0x0D60A27891250D7a7f78D6c279689164d704189E),
  ('{{dpi_eth}}', 18, 0x029849bbc0b1d93b85a8b6190e979fd38F5760E2, 0x36e4f71440EdF512EB410231e75B9281d4FcFC4c),
  ('{{dpi_usd}}', 8, 0xD2A593BF7594aCE1faD597adb697b5645d5edDB2, 0xA122f84935477142295F7451513e502D49316285),
  ('{{dpi_usd}}', 8, 0xD2A593BF7594aCE1faD597adb697b5645d5edDB2, 0x68f1B8317C19ff02fb68A8476C1D3f9Fc5139c0A),
  ('{{dydx_usd}}', 8, 0x478909D4D798f3a1F11fFB25E4920C959B4aDe0b, 0x6A0cCCA35f6ca00146547B949233C63441B34d7a),
  ('{{dydx_usd}}', 8, 0x478909D4D798f3a1F11fFB25E4920C959B4aDe0b, 0xe28371cd7a0c1687d2D956a567946688B48e5629),
  ('{{doodles_floor_price}}', 18, 0x027828052840a43Cc2D0187BcfA6e3D6AcE60336, 0x440C8fc45C7f00E09c2F437b95FC123888a3d951),
  ('{{enj_usd}}', 8, 0x23905C55dC11D609D5d11Dc604905779545De9a7, 0xCBbe4ff0d8add07CCe71afC0CcdF3492b8eaA76A),
  ('{{ens_usd}}', 8, 0x5C00128d4d1c2F4f652C267d7bcdD7aC99C16E16, 0x780f1bD91a5a22Ede36d4B2b2c0EcCB9b1726a28),
  ('{{eos_usd}}', 8, 0x10a43289895eAff840E8d45995BBa89f9115ECEe, 0xea7C55976844396f3bD4C89F66988b8b5Be5E96a),
  ('{{ern_usd}}', 8, 0x0a87e12689374A4EF49729582B474a1013cceBf8, 0xbDa0c715E5F153092A0d9d6dBBbDCc2beF892618),
  ('{{eth_btc}}', 8, 0xAc559F25B1619171CbC396a50854A3240b6A4e99, 0x0f00392FcB466c0E4E4310d81b941e07B4d5a079),
  ('{{eth_usd}}', 8, 0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419, 0xE62B71cf983019BFf55bC83B48601ce8419650CC),
  ('{{eurt_usd}}', 8, 0x01D391A48f4F7339aC64CA2c83a07C22F95F587a, 0x920E5DC12E7500c6571C63D4Bba19c62e99d6883),
  ('{{farm_eth}}', 18, 0x611E0d2709416E002A3f38085e4e1cf77c015921, 0x883Ba661FD9824778FF14a97F3A03eb324540201),
  ('{{fei_usd}}', 8, 0x31e0a88fecB6eC0a411DBe0e9E76391498296EE9, 0xc04126CF546146457C45009BCE5dA529eF960Fa1),
  ('{{fil_eth}}', 18, 0x0606Be69451B1C9861Ac6b3626b99093b713E801, 0x9965AD91B4877d29c246445011Ce370b3890C5C2),
  ('{{flow_usd}}', 8, 0xD9BdD9f5ffa7d89c846A5E3231a093AE4b3469D2, 0x3C640c857f1fF57ff4E24CfF1924F14A9bB9F2Ad),
  ('{{for_usd}}', 8, 0x456834f736094Fb0AAD40a9BBc9D4a0f37818A54, 0xf649bC5C0e99BDEb06702e3db242B9F93DE44462),
  ('{{forth_usd}}', 8, 0x7D77Fd73E468baECe26852776BeaF073CDc55fA0, 0xe2d6D8e799Df0a377FD14Ee18b95fd7cAa389017),
  ('{{fox_usd}}', 8, 0xccA02FFEFAcE21325befD6616cB4Ba5fCB047480, 0x49F3C586Df22Fd6146c22E5831907919dcb80527),
  ('{{fox_usd}}', 8, 0xccA02FFEFAcE21325befD6616cB4Ba5fCB047480, 0x02E59cE2921e982A481c6ddb709B76C33397Eb14),
  ('{{frax_eth}}', 18, 0x14d04Fff8D21bd62987a5cE9ce543d2F1edF5D3E, 0x56f98706C14DF5C290b02Cec491bB4c20834Bb51),
  ('{{fast_gas_gwei}}', 0, 0x169E633A2D1E6c10dD91238Ba11c4A708dfEF37C, 0x785433d8b06D77D68dF6be63944742130A4530d1),
  ('{{fluent_us_por}}', 8, 0xf623562437735E297C63B73c9e4417614147505C, 0xb6F94457113A091959E492Ef38bc3aE3cc475C63),
  ('{{gbpt_por}}', 18, 0xF6f5b570aB6E39E55558AfD8E1E30c5f20E6527E, 0x0a0aba8efAB65fDD3fa7e6afcb8128bCd6ffbdBF),
  ('{{gho_usd}}', 8, 0x3f12643D3f6f874d39C2a4c9f2Cd6f2DbAC877FC, 0x2E1D7e5Ba9A04ff2AA15be73b812fe1F8A43c3d7),
  ('{{glm_usd}}', 8, 0x83441C3A10F4D05de6e0f2E849A850Ccf27E6fa7, 0xa39b675ECc48E0681163f8788430e16b750d0f69),
  ('{{grt_eth}}', 18, 0x17D054eCac33D91F7340645341eFB5DE9009F1C1, 0x7531f77095Bed9d63cB3E9EA305111a7DCE969A2),
  ('{{grt_eth}}', 18, 0x17D054eCac33D91F7340645341eFB5DE9009F1C1, 0x91401cedCBFd9680cE193A5F54E716504233e998),
  ('{{gtc_eth}}', 18, 0x0e773A17a01E2c92F5d4c53435397E2bd48e215F, 0x0E27a36B2dFc0935A99Ba0c9C8E0F764c2da606C),
  ('{{gusd_eth}}', 18, 0x96d15851CBac05aEe4EFD9eA3a3DD9BDEeC9fC28, 0x9c2C487DAd6C8e5bb49dC6908a29D95a234FaAd8),
  ('{{gusd_usd}}', 8, 0xa89f5d2365ce98B3cD68012b6f503ab1416245Fc, 0x6a805f2580b8D75d40331c26C074c2c42961E7F2),
  ('{{hbtc_por}}', 18, 0x0A8cD0115B1EE87EbA5b8E06A9a15ED93e230f7a, 0x6aa553CB870a54BD62bb54E11f0B2c919925E726),
  ('{{high_usd}}', 8, 0xe2F95bC12FE8a3C35684Be7586C39fD7c0E5b403, 0xeA23780dc14d1aC9eb3AB203A9bb7C1A6660615E),
  ('{{ib01_usd}}', 8, 0x32d1463EB53b73C095625719Afa544D5426354cB, 0x5EE6Ee50c1cB3E8Da20eE83D57818184387433e8),
  ('{{ibta_usd}}', 8, 0xd27e6D02b72eB6FCe04Ad5690C419196B4EF2885, 0x5f8c943a29FFfC7Df8cE4001Cf1bedbCFC610476),
  ('{{idr_usd}}', 8, 0x91b99C9b75aF469a71eE1AB528e8da994A5D7030, 0x156710f56dC5F0C022505A9ffE95b0b51A7c5c9A),
  ('{{ilv_eth}}', 18, 0xf600984CCa37cd562E74E3EE514289e3613ce8E4, 0xc1F2929b9449Ef97c5A75fd10fD0542984422f8b),
  ('{{imx_usd}}', 8, 0xBAEbEFc1D023c0feCcc047Bff42E75F15Ff213E6, 0x3f00247Dc3bc14A8dCfA682318Ce566b1f34343A),
  ('{{inj_usd}}', 8, 0xaE2EbE3c4D20cE13cE47cbb49b6d7ee631Cd816e, 0x1a4E4B344125E7ef78de22b55FCeF5a4bc45f605),
  ('{{iotx_usd}}', 8, 0x96c45535d235148Dc3ABA1E48A6E3cFB3510f4E2, 0x910BD38d1C8D06d9c32b92AED3833DD503eE1321),
  ('{{jpegd_azuki_floor_price_eth}}', 18, 0xA9cdBbDE36803af377735233e6BD261cDA5aD11d, 0xd7Ca5ad3704150348E7Ddff8aa890A440f3b25CF),
  ('{{jpegd_bored_ape_floor_price_eth}}', 18, 0x0CA05B24795eb4f5bA5237e1D4470048cc0fE235, 0xc9460800d678cD6be9374c023A72ed0c2017AFF9),
  ('{{jpegd_bored_ape_kennel_club_floor_price_eth}}', 18, 0x7Bf3ad9582De40942C1EF876571d9864D71c548b, 0x75e7c3D9ccDae31D560Aba6d34d6d47BbA814De8),
  ('{{jpegd_chromie_floor_price_eth}}', 18, 0x639c3c1e3b5aa262b87e407779c866cC1406DDe6, 0x271C5B4542eEEb78b08681D30168B4E4359a893e),
  ('{{jpegd_clonex_floor_price_eth}}', 18, 0x13E6C463BEC76873E4e63ce5169e9a95b7e06801, 0xe3cEFdBf16950B171cb2Ad68F047d3BA2d92f91e),
  ('{{jpegd_cryptopunks_floor_price_eth}}', 18, 0x35f08E1b5a18F1F085AA092aAed10EDd47457484, 0x3D1fDFB6C9579D249d2bA6D85043C53Cac77fB3a),
  ('{{jpegd_doodle_floor_price_eth}}', 18, 0x68Ff67118F778Bd158DA8D49B156aC5Ad9d8c4Ed, 0x9359397f078D1A186C74E1963e861eB109B30D3b),
  ('{{jpegd_fidenza_floor_price_eth}}', 18, 0x2dE2EB5Fb9B8d7df45A9e144030c36128682c288, 0x54d2AB141e69bfA42E0808fAdD40ba1135f8591b),
  ('{{jpegd_meebits_floor_price_eth}}', 18, 0x6f9D4D55Eb44915674d9f708AE17F23b2ae79AAc, 0x5957c8962f9CA8BeDea67541F73aA72A5E90f9BF),
  ('{{jpegd_milady_floor_price_eth}}', 18, 0x5fB3912d73d55E656E2Dfb35B11696006f5A5745, 0x080874cf20e5219946B27778eE7CcBf31bF9F3A5),
  ('{{jpegd_mutant_ape_floor_price_eth}}', 18, 0xE6A7b525609bF47889ac9d0e964ebB640750a01C, 0x19Dfc7d6262D609fedA883C08BaF3F5273E5bCC3),
  ('{{jpegd_otherdeed_floor_price_eth}}', 18, 0x6bD37CB175B222E7ddFb90CCA170e0f2b21F2849, 0x308b6ECf13e90Ba323aFC9c678B13A94f84F77ca),
  ('{{jpegd_otherside_koda_floor_price_eth}}', 18, 0x24340E6e1b61BE416740b52fc776af7E0BDC56dD, 0x11786F4e2a6618430d61C36F83DD687bE8371c83),
  ('{{jpegd_pudgy_penguins_floor_price_eth}}', 18, 0xaC9962D846D431254C7B3Da3AA12519a1E2Eb5e7, 0xbFbCc713B8320D924079eff26fcC773353275F10),
  ('{{jpegd_ringers_floor_price_eth}}', 18, 0xd88b089f47d6f82e84589601fd7c329472077E08, 0xC90ab15E9127c4E2DAE9aC370c2Fd0c768C20ac2),
  ('{{krw_usd}}', 8, 0x01435677FB11763550905594A16B645847C1d0F3, 0x86e345D4113E1105053A81240C75b56B437dA6Ef),
  ('{{ksm_usd}}', 8, 0x06E4164E24E72B879D93360D1B9fA05838A62EB5, 0x630163B84674B2B404fB6036A510574F259c5Cb7),
  ('{{link_usd}}', 8, 0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c, 0x20807Cf61AD17c31837776fA39847A2Fa1839E81),
  ('{{lon_eth}}', 18, 0x13A8F2cC27ccC2761ca1b21d2F3E762445f201CE, 0xB82A0803DF982412dAeE9D82826395E3F0BeD1A2),
  ('{{lrc_eth}}', 18, 0x160AC928A16C93eD4895C2De6f81ECcE9a7eB7b4, 0x9405e02996Aa6f2176E2748EEfbCedd405870cee),
  ('{{lusd_usd}}', 8, 0x3D7aE7E594f2f2091Ad8798313450130d0Aba3a0, 0x27b97a63091d185cE056e1747624b9B92BAAD056),
  ('{{mana_usd}}', 8, 0x56a4857acbcfe3a66965c251628B1c9f1c408C19, 0x7Be21AeF96E2FAeB8Dc0d07306814319cA034cad),
  ('{{matic_usd}}', 8, 0x7bAC85A8a13A4BcD8abb3eB7d6b4d632c5a57676, 0x4B35F7854e1fd8291f4EC714aC3EBB1DeA450585),
  ('{{mayc_floor_price}}', 18, 0x1823C89715Fe3fB96A24d11c917aCA918894A090, 0xb17Eac46CF1B9C5fe2F707c8A47AFc4d208b3E83),
  ('{{mim_usd}}', 8, 0x7A364e8770418566e3eb2001A96116E6138Eb32F, 0x18f0112E30769961AF90FDEe0D1c6B27E6d72D92),
  ('{{mkr_usd}}', 8, 0xec1D1B3b0443256cc3860e24a46F108e699484Aa, 0x71Febc2F741F113af322e1B576eF005A4424574F),
  ('{{msft_usd}}', 8, 0x021Fb44bfeafA0999C7b07C4791cf4B859C3b431, 0x99a9422bdBf888fAd917b3a714103E896D3e2011),
  ('{{metis_healthcheck}}', 8, 0x3425455fe737cdaE8564640df27bbF2eCD56E584, 0x31c8b5A8F0d286a4Bfcf669E18393b18E22B140D),
  ('{{moonbirds_floor_price}}', 18, 0x9cd36E0E8D3C27d630D00406ACFC3463154951Af, 0x8d0003e5c1C8EB67e04023a21291cf01CFd2E4a1),
  ('{{nexus_weth_reserves}}', 18, 0xCc72039A141c6e34a779eF93AEF5eB4C82A893c7, 0xCA71bBe491079E138927f3f0AB448Ae8782d1DCa),
  ('{{ohm_eth}}', 18, 0x90c2098473852E2F07678Fe1B6d595b1bd9b16Ed, 0x87831da9319260B0B38dD39A73EBD4c2C10C588c),
  ('{{ohm_eth}}', 18, 0x90c2098473852E2F07678Fe1B6d595b1bd9b16Ed, 0x7009033C0d6702fd2dfAD3478d2AE4e3b6aCB966),
  ('{{ohmv2_eth}}', 18, 0x9a72298ae3886221820B1c878d12D872087D3a23, 0x9Aae856973A0Cafa084b82F7BC4C6C2893A9139b),
  ('{{optimism_healthcheck}}', 8, 0x59c2287c8E848310c809C061a1Be0d1556eFF4e2, 0x90f07EDF949673732178D9F305B8183524120ea8),
  ('{{otherdeed_for_otherside_floor_price}}', 18, 0x6e3A4376B4C8D3ba49602f8542D9D3C4A87ba901, 0xE308e892e153B899404928b6C705b7c8Da231F0F),
  ('{{perp_usd}}', 8, 0x01cE1210Fe8153500F60f7131d63239373D7E26C, 0xAcD3657b1D552623992aea368D9192C780B9d441),
  ('{{perp_usd}}', 8, 0x01cE1210Fe8153500F60f7131d63239373D7E26C, 0x608D4724F725845C2bbb1A27D7DCe341C9F85B00),
  ('{{pha_usd}}', 8, 0x2B1248028fe48864c4f1c305E524e2e6702eAFDF, 0xCB00334A422FC8538794f3CE0149540a95f9e228),
  ('{{pudgy_penguins_floor_price}}', 18, 0x9f2ba149c2A0Ee76043d83558C4E79E9F3E5731B, 0x1A93f0C2168DfeEF0801D85E74FB21F4534Ddfc8),
  ('{{rai_usd}}', 8, 0x483d36F6a1d063d580c7a24F9A42B346f3a69fbb, 0x2Abfc56AaA39be7a946ec39aAC5d452e30614dF1),
  ('{{rari_eth}}', 18, 0x2a784368b1D492f458Bf919389F42c18315765F5, 0x25C32A551C188Cb88a7067c254905191e83C712C),
  ('{{ren_usd}}', 8, 0x0f59666EDE214281e956cb3b2D0d69415AfF4A01, 0x3d0bB55D0D2F255d7A0EAb8A53a91b3369728E36),
  ('{{rep_eth}}', 18, 0xD4CE430C3b67b3E2F7026D86E7128588629e2455, 0x5d7d68D7c66a3Ac30e7727Ae380817a534c7bc89),
  ('{{req_usd}}', 8, 0x2F05888D185970f178f40610306a0Cc305e52bBF, 0x8127087BaD4fd28e1DAcbAfc3d3040E701b2B9A2),
  ('{{reth_eth}}', 18, 0x536218f9E9Eb48863970252233c8F271f554C2d0, 0x9cB248E68fb81d0CFE7D6B3265Fe6Bf123A71FE0),
  ('{{rpl_usd}}', 8, 0x4E155eD98aFE9034b7A5962f6C84c86d869daA9d, 0x5Df960959De45A2BA9DC11e6fD6F77107F43256C),
  ('{{rsr_usd}}', 8, 0x759bBC1be8F90eE6457C44abc7d443842a976d02, 0xA27CfD69345a6e121284a3C0ae07BB64b707cDD2),
  ('{{shib_eth}}', 18, 0x8dD1CD88F43aF196ae478e91b9F5E4Ac69A97C61, 0xB895192F5a49914ae760F01Ef92DB285d94C783E),
  ('{{shv_usd}}', 8, 0xc04611C43842220fd941515F86d1DDdB15F04e46, 0x9E1320991057c1246cba9F02c79f272a4Da892b3),
  ('{{sol_usd}}', 8, 0x4ffC43a60e009B551865A93d232E33Fce9f01507, 0xDf30249744A419891f822ea4a9E80cd76d7Fbd23),
  ('{{spell_usd}}', 8, 0x8c110B94C5f1d347fAcF5E1E938AB2db60E3c9a8, 0x070f15084600Aceace6D639CDDd0e341975D1e30),
  ('{{spell_usd}}', 8, 0x8c110B94C5f1d347fAcF5E1E938AB2db60E3c9a8, 0x8640b23468815902e011948F3aB173E1E83f9879),
  ('{{stbt_por}}', 18, 0xad4A9bED9a5E2c1c9a6E43D35Db53c83873dd901, 0x040d003e56566aEd1D0cCdc54c551f76848bD219),
  ('{{steth_eth}}', 18, 0x86392dC19c0b719886221c78AB11eb8Cf5c52812, 0x716BB759A5f6faCdfF91F0AfB613133d510e1573),
  ('{{steth_usd}}', 8, 0xCfE54B5cD566aB89272946F602D76Ea879CAb4a8, 0xdA31bc2B08F22AE24aeD5F6EB1E71E96867BA196),
  ('{{stg_usd}}', 8, 0x7A9f34a0Aa917D438e9b6E630067062B7F8f6f3d, 0x73455B8aCd6d205544cbC034a6f6cAB58c56ef47),
  ('{{sushi_usd}}', 8, 0xCc70F09A6CC17553b2E31954cD36E4A2d89501f7, 0x3CF055335b521863A62fB4374972560e3e55a193),
  ('{{sushi_usd}}', 8, 0xCc70F09A6CC17553b2E31954cD36E4A2d89501f7, 0xbd6C554554834ee97828B6DA732dCa7461DDf9d4),
  ('{{swell_eth_por}}', 18, 0x60cbE8D88EF519cF3C62414D76f50818D211fea1, 0x477716B8e95749bF31ce26cF4e4E4Af87b8Acf59),
  ('{{synthetix_aggregator_debt_ratio}}', 27, 0x0981af0C002345c9C5AD5efd26242D0cBe5aCA99, 0xc7BB32a4951600FBac701589C73e219b26Ca2DFC),
  ('{{synthetix_aggregator_issued_synths}}', 18, 0xbCF5792575bA3A875D8C406F4E7270f51a902539, 0x59CCf62B862f99B5aEd8857FBAdB7F895f6c59D5),
  ('{{tusd_usd}}', 8, 0xec746eCF986E2927Abd291a2A1716c940100f8Ba, 0x98953e9C76573e06ec265Bdde1dbB89fa02d56d3),
  ('{{tusd_por}}', 18, 0x81243627cC533Ca6CF6F23c3f61add1D0f626674, 0x4d76Ae11EEF9cEf363300Abf66b599BDE4aBb33e),
  ('{{tusd_reserves}}', 18, 0xBE456fd14720C3aCCc30A2013Bffd782c9Cb75D5, 0xAC099D59755982757537F13c7c4Ae8c8d9F030B9),
  ('{{total_marketcap_usd}}', 8, 0xEC8761a0A73c34329CA5B1D3Dc7eD07F30e836e2, 0x9257D83A0DdA413cA24F66dD32A056Bc2eBAFd2e),
  ('{{uma_eth}}', 18, 0xf817B69EA583CAFF291E287CaE00Ea329d22765C, 0x68a371D12213a1EbDd5fa9a2EE5519E6B73F1E37),
  ('{{uni_usd}}', 8, 0x553303d460EE0afB37EdFf9bE42922D8FF63220e, 0x373BCe97bec13BfA8A5f07Cc578EC2D77f80c589),
  ('{{usdd_usd}}', 8, 0x0ed39A19D2a68b722408d84e4d970827f61E6c0A, 0x589a85FC02EB6bB86D1c84c1a75abbB012C661De),
  ('{{usdp_usd}}', 8, 0x09023c0DA49Aaf8fc3fA3ADF34C6A7016D38D5e3, 0xF3d70857B489Ecc6768D0982B773E1Cba9E1f00b),
  ('{{ust_eth}}', 18, 0xa20623070413d42a5C01Db2c8111640DD7A5A03a, 0x4a81f77C8BBcA2CbA8110279cDbC9F1A8D3eAE6B),
  ('{{ust_usd}}', 8, 0x8b6d9085f310396C6E4f0012783E9f850eaa8a82, 0x01b87e7fF78022A70394d3C6Dd127D0c709e3beA),
  ('{{ust_usd}}', 8, 0x8b6d9085f310396C6E4f0012783E9f850eaa8a82, 0x5EDd5F803b831b47715aD3e11a90dD244F0cD0a9),
  ('{{veefriends_floor_price}}', 18, 0x35bf6767577091E7f04707c0290b3f889e968307, 0xe0552DC960366F67Da00CB3d9DF441F24B5C2AC1),
  ('{{wbtc_btc}}', 8, 0xfdFD9C85aD200c506Cf9e21F1FD8dd01932FBB23, 0xD7623f1d24b35c392862fB67C9716564A117C9DE),
  ('{{wbtc_por}}', 8, 0xa81FE04086865e63E12dD3776978E49DEEa2ea4e, 0xB622b7D6d9131cF6A1230EBa91E5da58dbea6F59),
  ('{{wing_usd}}', 8, 0x134fE0a225Fb8e6683617C13cEB6B3319fB4fb82, 0xc29d104A418a08407f9f2CDb614c1CDCf82986e0),
  ('{{wing_usd}}', 8, 0x134fE0a225Fb8e6683617C13cEB6B3319fB4fb82, 0x2c9A8c2caEb80FEb24048587a10BFB6aeFF601c5),
  ('{{world_of_women_floor_price}}', 18, 0xDdf0B85C600DAF9e308AFed9F597ACA212354764, 0x45B68d24Df514BF13a838d88bE4363F8011719de),
  ('{{xcn_usd}}', 8, 0xeb988B77b94C186053282BfcD8B7ED55142D3cAB, 0xD6A3a9Bb4bd49DdB2374CA58Edf47a8bB63Af3d2),
  ('{{yfi_usd}}', 8, 0xA027702dbb89fbd58938e4324ac03B58d812b0E1, 0xcac109af977AC94929A5dD37ed8Af763BAD78151),
  ('{{zrx_usd}}', 8, 0x2885d15b8Af22648b98B122b22FDF4D2a56c6023, 0x4Dde220fF2690A350b0Ea9404F35C8f3Ad012584),
  ('{{efil_por}}', 18, 0x8917800a6BDd8fA8b7c94E25aE2219Db28050622, 0xD423C9A9AD8c21C97bdeE2E74F8098625aa4f329)
) a (feed_name, decimals, proxy_address, aggregator_address)
