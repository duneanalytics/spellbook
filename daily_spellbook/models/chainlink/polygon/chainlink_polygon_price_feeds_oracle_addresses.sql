{{
  config(
    
    alias='price_feeds_oracle_addresses',
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "chainlink",
                                \'["msilb7","0xroll","linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set _1inch_usd = '1INCH / USD' %}
{% set aapl_usd = 'AAPL / USD' %}
{% set aave_network_emergency_count_polygon_ = 'AAVE Network Emergency Count (Polygon)' %}
{% set aed_usd = 'AED / USD' %}
{% set ageur_usd = 'AGEUR / USD' %}
{% set alcx_usd = 'ALCX / USD' %}
{% set alpha_usd = 'ALPHA / USD' %}
{% set amkt_por = 'AMKT PoR' %}
{% set amzn_usd = 'AMZN / USD' %}
{% set ant_usd = 'ANT / USD' %}
{% set ape_usd = 'APE / USD' %}
{% set avax_usd = 'AVAX / USD' %}
{% set axs_usd = 'AXS / USD' %}
{% set badger_eth = 'BADGER / ETH' %}
{% set badger_usd = 'BADGER / USD' %}
{% set bal_eth = 'BAL / ETH' %}
{% set bal_usd = 'BAL / USD' %}
{% set brl_usd = 'BRL / USD' %}
{% set btc_eth = 'BTC / ETH' %}
{% set cbeth_eth = 'CBETH / ETH' %}
{% set cel_usd = 'CEL / USD' %}
{% set cgt_por_eth_ = 'CGT PoR (ETH)' %}
{% set chz_usd = 'CHZ / USD' %}
{% set cny_usd = 'CNY / USD' %}
{% set cvx_usd = 'CVX / USD' %}
{% set calculated_maticx_usd = 'Calculated MaticX / USD' %}
{% set calculated_stmatic_usd = 'Calculated stMATIC / USD' %}
{% set dfi_usd = 'DFI / USD' %}
{% set dgb_usd = 'DGB / USD' %}
{% set dodo_usd = 'DODO / USD' %}
{% set doge_usd_total_marketcap = 'DOGE-USD Total Marketcap' %}
{% set dpi_eth = 'DPI / ETH' %}
{% set enj_usd = 'ENJ / USD' %}
{% set farm_usd = 'FARM / USD' %}
{% set fb_usd = 'FB / USD' %}
{% set fil_usd = 'FIL / USD' %}
{% set fis_usd = 'FIS / USD' %}
{% set ftm_usd = 'FTM / USD' %}
{% set ftt_usd = 'FTT / USD' %}
{% set ghst_eth = 'GHST / ETH' %}
{% set ghst_usd = 'GHST / USD' %}
{% set googl_usd = 'GOOGL / USD' %}
{% set grt_usd = 'GRT / USD' %}
{% set hbar_usd = 'HBAR / USD' %}
{% set icp_usd = 'ICP / USD' %}
{% set idr_usd = 'IDR / USD' %}
{% set ils_usd = 'ILS / USD' %}
{% set inr_usd = 'INR / USD' %}
{% set kava_usd = 'KAVA / USD' %}
{% set klay_usd = 'KLAY / USD' %}
{% set krw_usd = 'KRW / USD' %}
{% set link_matic = 'LINK / MATIC' %}
{% set mim_usd = 'MIM / USD' %}
{% set mimatic_usd = 'MIMATIC / USD' %}
{% set mkr_usd = 'MKR / USD' %}
{% set mln_eth = 'MLN / ETH' %}
{% set msft_usd = 'MSFT / USD' %}
{% set mxn_usd = 'MXN / USD' %}
{% set nexo_usd = 'NEXO / USD' %}
{% set nzd_usd = 'NZD / USD' %}
{% set ocean_usd = 'OCEAN / USD' %}
{% set ogn_usd = 'OGN / USD' %}
{% set ohm_index = 'OHM Index' %}
{% set ohmv2_usd = 'OHMv2 / USD' %}
{% set om_usd = 'OM / USD' %}
{% set php_usd = 'PHP / USD' %}
{% set pla_usd = 'PLA / USD' %}
{% set pln_usd = 'PLN / USD' %}
{% set quick_eth = 'QUICK / ETH' %}
{% set quick_usd = 'QUICK / USD' %}
{% set rai_usd = 'RAI / USD' %}
{% set se_usd = 'SE / USD' %}
{% set sek_usd = 'SEK / USD' %}
{% set shib_usd = 'SHIB / USD' %}
{% set slp_usd = 'SLP / USD' %}
{% set sol_usd = 'SOL / USD' %}
{% set spy_us = 'SPY.US' %}
{% set steth_usd = 'STETH / USD' %}
{% set storj_usd = 'STORJ / USD' %}
{% set sushi_eth = 'SUSHI / ETH' %}
{% set thb_usd = 'THB / USD' %}
{% set theta_usd = 'THETA / USD' %}
{% set try_usd = 'TRY / USD' %}
{% set tsla_usd = 'TSLA / USD' %}
{% set tzs_usd = 'TZS / USD' %}
{% set vnd_usd = 'VND / USD' %}
{% set woo_usd = 'WOO / USD' %}
{% set wsteth_eth = 'WSTETH / ETH' %}
{% set xag_usd = 'XAG / USD' %}
{% set xmr_usd = 'XMR / USD' %}
{% set xpt_usd = 'XPT / USD' %}
{% set xtz_usd = 'XTZ / USD' %}
{% set yfi_usd = 'YFI / USD' %}
{% set zar_usd = 'ZAR / USD' %}
{% set ibbtc_pricepershare = 'ibBTC PricePerShare' %}
{% set wsteth_steth_exchange_rate = 'wstETH-stETH Exchange Rate' %}

SELECT
   'polygon' as blockchain,
   feed_name,
   CAST(decimals AS BIGINT) as decimals,
   proxy_address,
   aggregator_address
FROM (values
  ('{{_1inch_usd}}', 8, 0x443C5116CdF663Eb387e72C688D276e702135C87, 0x60A47cC34342bc43C308B67D5836D9116A797D6A),
  ('{{aapl_usd}}', 8, 0x7E7B45b08F68EC69A99AAb12e42FcCB078e10094, 0x9fC6ee32430DC0baD47742C70eCA0a848D2b897F),
  ('{{aave_network_emergency_count_polygon_}}', 0, 0xDAFA1989A504c48Ee20a582f2891eeB25E2fA23F, 0x889E905D743aF41BEAbd77bF699a33c0cEf981dA),
  ('{{aed_usd}}', 8, 0x3fd911749Fce21a38704B76FFaBcB6BeF2567F2E, 0x81dD0F721fF3cd23f3CFcCa10A7dcc06fF3B5505),
  ('{{ageur_usd}}', 8, 0x9b88d07B2354eF5f4579690356818e07371c7BeD, 0x63a39D35751E8D3e80734BcDC755B2145718385D),
  ('{{alcx_usd}}', 8, 0x5DB6e61B6159B20F068dc15A47dF2E5931b14f29, 0xc4AF1C7744131BfE4Cf86ca2b1688d7F1f428Eaf),
  ('{{alpha_usd}}', 8, 0x289833F252eaB98582D62db94Bd75aB48AD9CF0D, 0x2A96a9939b4Bf779b23f46B205727D602fE7bD5d),
  ('{{amkt_por}}', 18, 0x32640253A3d0Fc25597D8a784a839311FF404C83, 0x951704608E450D290f1D8deC2d9509fC22fa8086),
  ('{{amzn_usd}}', 8, 0xf9184b8E5da48C19fA4E06f83f77742e748cca96, 0xd64DC710a0E8fe2944eC1a845AF116c2Cc8C81dA),
  ('{{ant_usd}}', 8, 0x213b030E24C906ee3b98EC7538Cc6D3D3C82aF55, 0xfC8fb8Bd285FF358FEF481b45dbc7450C0F8f89d),
  ('{{ape_usd}}', 8, 0x2Ac3F3Bfac8fC9094BC3f0F9041a51375235B992, 0xB24ACd0C92014920Aa233F78331e47aEd3B5f204),
  ('{{avax_usd}}', 8, 0xe01eA2fbd8D76ee323FbEd03eB9a8625EC981A10, 0xE3a36141cb950eb56DEC7383F2d9234Bbdc69B6e),
  ('{{axs_usd}}', 8, 0x9c371aE34509590E10aB98205d2dF5936A1aD875, 0xe6c6Fd2b0959B2d5385b1EAb277a57FcBA6A2203),
  ('{{axs_usd}}', 8, 0x9c371aE34509590E10aB98205d2dF5936A1aD875, 0xE2c89f4Bfab45B574942523FEAA430C6e193725B),
  ('{{badger_eth}}', 18, 0x82C9d4E88862f194C2bd874a106a90dDD0D35AAB, 0xbE1c032bD5Fcd0285Af538235eE671B271c98d5e),
  ('{{badger_usd}}', 8, 0xF626964Ba5e81405f47e8004F0b276Bb974742B5, 0xC1E913be9AD8bBAAC5c5cc4aAdafdf63BFC4ec8d),
  ('{{bal_eth}}', 18, 0x03CD157746c61F44597dD54C6f6702105258C722, 0x7C66609Db07C8983f324098DDc53F03af716aba7),
  ('{{bal_usd}}', 8, 0xD106B538F2A868c28Ca1Ec7E298C3325E0251d66, 0xd0CCf213410578DF4EC5EB0157234120B30d2f81),
  ('{{brl_usd}}', 8, 0xB90DA3ff54C3ED09115abf6FbA0Ff4645586af2c, 0x6DBd1be1a83005d26b582D61937b406300B05A8F),
  ('{{btc_eth}}', 18, 0x19b0F0833C78c0848109E3842D34d2fDF2cA69BA, 0xAA7B8f063457756E32f782EfE981908c2De83C68),
  ('{{cbeth_eth}}', 18, 0x0a6a03CdF7d0b48d4e4BA8e362A4FfC3aAC4f3c0, 0x508e317956cA8Cc9c824c1786c0c963699d99f75),
  ('{{cel_usd}}', 8, 0xc9ECF45956f576681bDc01F79602A79bC2667B0c, 0x5e079671301375Be7AfC9bc0B9958F79426dE847),
  ('{{cgt_por_eth_}}', 8, 0x4e9fc7480c16F3FE5d956C0759eE6b4808d1F5D7, 0x586d26055914143a0100E26541e1dd47D570045b),
  ('{{chz_usd}}', 8, 0x2409987e514Ad8B0973C2b90ee1D95051DF0ECB9, 0x2051Efb579C4A014Cbc0d6Af050768cBDB8A2478),
  ('{{cny_usd}}', 8, 0x04bB437Aa63E098236FA47365f0268547f6EAB32, 0xF07eac7A48Eb772613479D6A8Fc42675f1BeFb47),
  ('{{cvx_usd}}', 8, 0x5ec151834040B4D453A1eA46aA634C1773b36084, 0x6729B40433A3cC07EEa99a57452824638F01416c),
  ('{{calculated_maticx_usd}}', 8, 0x5d37E4b374E6907de8Fc7fb33EE3b0af403C7403, 0x95622DC91570EB3b1301a4EBAb5d3eb6BE1fa38a),
  ('{{calculated_stmatic_usd}}', 8, 0x97371dF4492605486e23Da797fA68e55Fc38a13f, 0x0faf504bee22AF6E92D6697Af2EAfB9941a1712D),
  ('{{dfi_usd}}', 8, 0x3CD95aB66D27736d09994c0555Ef488E496a81b2, 0x8ad3E59aD44021668EF38cCC2aAfab85D45697F4),
  ('{{dgb_usd}}', 8, 0x4205eC5fd179A843caa7B0860a8eC7D980013359, 0x40C0819f7cbF6d2DEcCc9a88137d53fA2231dcAc),
  ('{{dodo_usd}}', 8, 0x59161117086a4C7A9beDA16C66e40Bdaa1C5a8B6, 0x908e90dcb4541d39D5b52CB84EB8ae3503Da046f),
  ('{{dodo_usd}}', 8, 0x59161117086a4C7A9beDA16C66e40Bdaa1C5a8B6, 0x9d2ef3aBb9e7e29A1a5A1Ab9fd319987bEa949c8),
  ('{{doge_usd_total_marketcap}}', 8, 0xbd238a35Fb47aE22F0cC551f14ffB8E8f04FCA21, 0xF7c7FB4b2C72dc1e118cE9ed38CF1D3d9Ea206C1),
  ('{{dpi_eth}}', 18, 0xC70aAF9092De3a4E5000956E672cDf5E996B4610, 0x02228Ab6bA5FfeE5ba15cd1477987FF4D4BfEce4),
  ('{{enj_usd}}', 8, 0x440A341bbC9FA86aA60A195e2409a547e48d4C0C, 0x24e54b4752392F48c0F4B6cc291b10bcaeD0c28a),
  ('{{farm_usd}}', 8, 0xDFb138ba3A6CCe675A6F5961323Be31eE42E40ff, 0xF1ae47478Cf731788Be7D1444DFe351146BDE3ed),
  ('{{fb_usd}}', 8, 0x5b4586C911144A947D7814Fd71fe0872b8334748, 0x6a261a56A644B27C2236c64C1F6638C968D7B9DD),
  ('{{fil_usd}}', 8, 0xa07703E5C2eD1516107c7c72A494493Dcb99C676, 0x39B46Cb14BCf5cd1Cf8129Db3eD0ceaa2f3bAa9a),
  ('{{fis_usd}}', 8, 0x18617D05eE1692Ad7EAFee9839459da16097AFd8, 0xDeDc34A2B7C192E9498A76D758c68b1CA14192B1),
  ('{{ftm_usd}}', 8, 0x58326c0F831b2Dbf7234A4204F28Bba79AA06d5f, 0x90f49083a5344979d8983cA95fBd107b3FF5cF3e),
  ('{{ftt_usd}}', 8, 0x817A7D43f0277Ca480AE03Ec76Fc63A2EC7114bA, 0xADD04304E4d88249670BEeE585b6188aD229b431),
  ('{{ftt_usd}}', 8, 0x817A7D43f0277Ca480AE03Ec76Fc63A2EC7114bA, 0x4cff0A023467949767009039F510ee724281c621),
  ('{{ghst_eth}}', 18, 0xe638249AF9642CdA55A92245525268482eE4C67b, 0xeC35E6F084cE365A819E99bCd1F89319e519Fdf3),
  ('{{ghst_usd}}', 8, 0xDD229Ce42f11D8Ee7fFf29bDB71C7b81352e11be, 0xA25aA6588c0311b9dB11c2887d9AcbB6b5e3d1B0),
  ('{{googl_usd}}', 8, 0x1b32682C033b2DD7EFdC615FA82d353e254F39b5, 0x550D316147a2B1e51E35B1102EBE7746C23042A6),
  ('{{grt_usd}}', 8, 0x3FabBfb300B1e2D7c9B84512fe9D30aeDF24C410, 0x0C6ff30968226be88430eeBFcbA7F217d81f370b),
  ('{{hbar_usd}}', 8, 0xC5878bDf8a89FA3bF0DC8389ae8EE6DE601D01bC, 0x57f9c3522DB39bdD095319c85A184e8Bb13FcEC0),
  ('{{icp_usd}}', 8, 0x84227A76a04289473057BEF706646199D7C58c34, 0x0deC46bfDa8Ce29bFa6bE4343Aa44d9beFC71E90),
  ('{{idr_usd}}', 8, 0x80a5cb83ce268Ed11a6EFC4bBF0beC39dF35Db21, 0xEdf55089c4171bf245016f4D9C46a6E326fAf923),
  ('{{ils_usd}}', 8, 0x8d5eB34C509261533235b91350d359EdcB969D33, 0x9c86EAe2A3f07aBb97b8c699467Bb90Db58d75aE),
  ('{{inr_usd}}', 8, 0xDA0F8Df6F5dB15b346f4B8D1156722027E194E60, 0x54D81825C7ba6766d8770Ec8aE9f786E700F6Df2),
  ('{{kava_usd}}', 8, 0x7899dd75C329eFe63e35b02bC7d60D3739FB23c5, 0x180fF2978Cd4f0cf7B7890f354eE5a986a31Be59),
  ('{{klay_usd}}', 8, 0x86F87CB74238a6f24606534A2fCc05469Eb2bcF5, 0xcEBf62b33FC637882B80e41f71d679fb331206B0),
  ('{{krw_usd}}', 8, 0x24B820870F726dA9B0D83B0B28a93885061dbF50, 0xFd54f97A6C408561b5Df798c04ae08B27cA0d7F7),
  ('{{link_matic}}', 18, 0x5787BefDc0ECd210Dfa948264631CD53E68F7802, 0x817C00aFc51e6574ACaA718336FB4414eBC87Fdb),
  ('{{mim_usd}}', 8, 0xd133F916e04ed5D67b231183d85Be12eAA018320, 0x9b3F311bD715A4E00dc11b4e21D5389de455bC13),
  ('{{mimatic_usd}}', 8, 0xd8d483d813547CfB624b8Dc33a00F2fcbCd2D428, 0x1c367A2D0a1E6c13dA55CAb82484D4CD8dc292e2),
  ('{{mkr_usd}}', 8, 0xa070427bF5bA5709f70e98b94Cb2F435a242C46C, 0xE41B5D02E64b165e77f12b72Bf80B56d076000CF),
  ('{{mln_eth}}', 18, 0xB89D583B72aBF9C3a7e6e093251C2fCad3365312, 0xbacad8B83716776B3252730487a8c81f5f1D9a00),
  ('{{msft_usd}}', 8, 0xC43081d9EA6d1c53f1F0e525504d47Dd60de12da, 0xbb9D50D19dB23Df32277545E0ecE65A93BEcE87D),
  ('{{mxn_usd}}', 8, 0x171b16562EA3476F5C61d1b8dad031DbA0768545, 0x2E2Ed40Fc4f1774Def278830F8fe3b6e77956Ec8),
  ('{{nexo_usd}}', 8, 0x666bb13b3ED3816504E8c30D0F9B9C16b371774b, 0xe2d792d64A36797f8d3E0F150B82d1E35Da76136),
  ('{{nzd_usd}}', 8, 0xa302a0B8a499fD0f00449df0a490DedE21105955, 0xE63032a70f6Eb617970829FbFa365D7C44BDbBbf),
  ('{{ocean_usd}}', 8, 0xdcda79097C44353Dee65684328793695bd34A629, 0x21B701aadF13300E944451073CA6dDB1af1e29a0),
  ('{{ogn_usd}}', 8, 0x8Ec0eC2e0F26D8253ABf39Db4B1793D76B49C6D5, 0x0961c007ffaAb3C7357f01e6BcE2386e340f1D0a),
  ('{{ohm_index}}', 9, 0xc08f70c26ab8C659EaF259c51a0F7ae22758c6ac, 0x2C7b866EFd8d4D136DE9826C3B7102496502c1cc),
  ('{{ohmv2_usd}}', 8, 0x4cE90F28C6357A7d3F47D680723d18AF3684cD00, 0xEE3563f684D74105d40d649f2e0150F748fE947C),
  ('{{ohmv2_usd}}', 8, 0x4cE90F28C6357A7d3F47D680723d18AF3684cD00, 0x03fE6917367CdE98039627FF3B8c7e61c684E76D),
  ('{{om_usd}}', 8, 0xc86105DccF9BD629Cea7Fd41f94c6050bF96D57F, 0x39a920f1eaa5a02839AB67802850d6e12bfdA5a5),
  ('{{php_usd}}', 8, 0x218231089Bebb2A31970c3b77E96eCfb3BA006D1, 0x8A2355Ec4678186164dc17DFC2C5D0d083d7Fd66),
  ('{{pla_usd}}', 8, 0x24C0e0FC8cCb21e2fb3e1A8A4eC4b29458664f79, 0x21ae958373cf1D3A82D095b70c205a78a6F27Fb2),
  ('{{pln_usd}}', 8, 0xB34BCE11040702f71c11529D00179B2959BcE6C0, 0x08f8D217e6F07aE423a2Ad2ffb226FfCB577708d),
  ('{{quick_eth}}', 18, 0x836a579B39d22b2147c1C229920d27880C915578, 0x836faa493e68faC2dd6b9250Ace9666fd48c4f09),
  ('{{quick_usd}}', 8, 0xa058689f4bCa95208bba3F265674AE95dED75B6D, 0x279108b32171D1D2eF2728d2AaE19b4e314687CC),
  ('{{rai_usd}}', 8, 0x7f45273fD7C644714825345670414Ea649b50b16, 0xe8abfC228Fc42d50a50B47C67AD9226349A01405),
  ('{{se_usd}}', 8, 0xcc73e00db7a6FD589a30BbE2E957086b8d7D3331, 0x0D40d2791126EADD2467ef6CbD4b68461c10Ff86),
  ('{{sek_usd}}', 8, 0xbd92B4919ae82be8473859295dEF0e778A626302, 0x542d2AF7F89a61205f3da2d3d13e29b56bDE7B46),
  ('{{shib_usd}}', 8, 0x3710abeb1A0Fc7C2EC59C26c8DAA7a448ff6125A, 0x2d8a85Fe1A6F288653246Ca08b8160378A8AE957),
  ('{{slp_usd}}', 8, 0xBB3eF70953fC3766bec4Ab7A9BF05B6E4caf89c6, 0x483cEBbda762eeE5B508dbE7179a2aF5A179eFC3),
  ('{{sol_usd}}', 8, 0x10C8264C0935b3B9870013e057f330Ff3e9C56dC, 0x37b557Dd3d3552C4DAA4dA935cf5bf2f3d04c8bF),
  ('{{spy_us}}', 8, 0x187c42f6C0e7395AeA00B1B30CB0fF807ef86d5d, 0x066Fe9d3A4B77bD48165f68aDBcd1EAa9eb1F7C9),
  ('{{steth_usd}}', 8, 0x87eF348CADd1Ed7cc7A5F4Fefb20325216AA2cEb, 0x322fd65e428bBE23782c52B9dC7Fb9D15D605011),
  ('{{storj_usd}}', 8, 0x0F1d5Bd7be9B30Fc09E110cd6504Bd450e53cb0E, 0xB1a56484BC2C6ba874C386CAa8381310fAf8985d),
  ('{{sushi_eth}}', 18, 0x17414Eb5159A082e8d41D243C1601c2944401431, 0x5826BDdE4E50B2DC78F62103E921B3DcD14D4FD7),
  ('{{thb_usd}}', 8, 0x5164Ad28fb12a5e55946090Ec3eE1B748AFb3785, 0xE70217D715b19190426017D282b0d7c200A8B45b),
  ('{{theta_usd}}', 8, 0x38611b09F8f2D520c14eA973765C225Bf57B9Eac, 0x953D8c16Fd4f22951c2f497669c6869b86b4e60E),
  ('{{try_usd}}', 8, 0xd78325DcA0F90F0FFe53cCeA1B02Bb12E1bf8FdB, 0x39E5E33C923bB56a7e2Ae644564f94b80630F3e3),
  ('{{tsla_usd}}', 8, 0x567E67f456c7453c583B6eFA6F18452cDee1F5a8, 0xA7cc6e5285cbCBbC61f7EAed4299f3a04E8ead65),
  ('{{tzs_usd}}', 8, 0xE6d13eF6Fb49230791C0F21927f091F2B8E2c566, 0x4f1Ea89A64b56287Ec74a0b1F59aFEC5ea7acdf3),
  ('{{vnd_usd}}', 8, 0x0Cf1D8c6651F4188E55fCe6AB25261948108F197, 0x3aE204993812121bf0C54e8B993e59A6978fbB58),
  ('{{woo_usd}}', 8, 0x6a99EC84819FB7007dd5D032068742604E755c56, 0xB16B1Ee56c70CF1FDD1E32D092045d08E5be4693),
  ('{{wsteth_eth}}', 18, 0x10f964234cae09cB6a9854B56FF7D4F38Cda5E6a, 0xeC43A133a79EAAeDac467E2413Ce824896005157),
  ('{{xag_usd}}', 8, 0x461c7B8D370a240DdB46B402748381C3210136b3, 0x00a27E2f64dE7B05E9ddF7aD6bA916d78458c8c7),
  ('{{xmr_usd}}', 8, 0xBE6FB0AB6302B693368D0E9001fAF77ecc6571db, 0x7aB0b2835f71ad2a31056007F651C897E5EE148A),
  ('{{xpt_usd}}', 8, 0xA6813d97eB2E0b50d0111385011a884097F74B30, 0x5e5e0Be97998268F10629ae0fCeD6622Be10DD53),
  ('{{xtz_usd}}', 8, 0x691e26AB58ff05800E028b0876A41B720b26FC65, 0xb6c02600D9956EDd226E87bB6F82cEa1ead8822F),
  ('{{yfi_usd}}', 8, 0x9d3A43c111E7b2C6601705D9fcF7a70c95b1dc55, 0x633c4dfD8e11008eB9e245ad4B84Cb76F197FD1b),
  ('{{zar_usd}}', 8, 0xd4a120c26d57B910C56c910CdD13EeBFA3135502, 0x88245775029Dc400a28371A77Cdbb9f15dCbB67c),
  ('{{ibbtc_pricepershare}}', 18, 0xc3E676E68dB28c9Fb2199f25B60560723237cc76, 0xc86dE80Ae2626664C46cBfB45366b59B405D1f9F),
  ('{{wsteth_steth_exchange_rate}}', 18, 0x3Ea1eC855fBda8bA0396975eC260AD2e9B2Bc01c, 0x874Fd3B6F91d1DFF7850EC39ACC1172006C3c7a3)
) a (feed_name, decimals, proxy_address, aggregator_address)
