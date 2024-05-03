{{
  config(
    alias='price_feeds_oracle_addresses',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set aave_usd = 'AAVE / USD' %}
{% set ageur_eur = 'AGEUR / EUR' %}
{% set alcx_eth = 'ALCX / ETH' %}
{% set ankr_usd = 'ANKR / USD' %}
{% set ape_eth = 'APE / ETH' %}
{% set ape_usd = 'APE / USD' %}
{% set apy_tvl = 'APY TVL' %}
{% set arb_usd = 'ARB / USD' %}
{% set arkb_reserves = 'ARKB Reserves' %}
{% set azuki_floor_price_eth = 'Azuki Floor Price / ETH' %}
{% set bal_usd = 'BAL / USD' %}
{% set bat_eth = 'BAT / ETH' %}
{% set bat_usd = 'BAT / USD' %}
{% set beanz_official_floor_price = 'BEANZ Official Floor Price' %}
{% set bnb_usd = 'BNB / USD' %}
{% set btc_usd = 'BTC / USD' %}
{% set base_healthcheck = 'Base Healthcheck' %}
{% set bored_ape_yacht_club_floor_price_eth = 'Bored Ape Yacht Club Floor Price / ETH' %}
{% set c3m_eur = 'C3M / EUR' %}
{% set cake_usd = 'CAKE / USD' %}
{% set cbeth_eth = 'CBETH / ETH' %}
{% set comp_usd = 'COMP / USD' %}
{% set crvusd_usd = 'CRVUSD / USD' %}
{% set cspx_usd = 'CSPX / USD' %}
{% set cvx_eth = 'CVX / ETH' %}
{% set cvx_usd = 'CVX / USD' %}
{% set cachegold_por_usd = 'CacheGold PoR USD' %}
{% set calculated_xsushi_eth = 'Calculated XSUSHI / ETH' %}
{% set clonex_floor_price = 'CloneX Floor Price' %}
{% set consumer_price_index = 'Consumer Price Index' %}
{% set coolcats_floor_price = 'CoolCats Floor Price' %}
{% set cryptopunks_floor_price_eth = 'CryptoPunks Floor Price / ETH' %}
{% set cryptoadz_floor_price = 'Cryptoadz Floor Price' %}
{% set dai_usd = 'DAI / USD' %}
{% set dpi_eth = 'DPI / ETH' %}
{% set dpi_usd = 'DPI / USD' %}
{% set doodles_floor_price = 'Doodles Floor Price' %}
{% set ens_usd = 'ENS / USD' %}
{% set eth_btc = 'ETH / BTC' %}
{% set eth_usd = 'ETH / USD' %}
{% set ethx_eth = 'ETHx / ETH' %}
{% set eurr_reserves = 'EURR Reserves' %}
{% set fdusd_usd = 'FDUSD / USD' %}
{% set fil_eth = 'FIL / ETH' %}
{% set frax_eth = 'FRAX / ETH' %}
{% set fast_gas_gwei = 'Fast Gas / Gwei' %}
{% set gbpt_por = 'GBPT PoR' %}
{% set gho_usd = 'GHO / USD' %}
{% set grt_eth = 'GRT / ETH' %}
{% set hbtc_por = 'HBTC PoR' %}
{% set high_usd = 'HIGH / USD' %}
{% set ib01_usd = 'IB01 / USD' %}
{% set ibta_usd = 'IBTA / USD' %}
{% set idr_usd = 'IDR / USD' %}
{% set imx_usd = 'IMX / USD' %}
{% set jpegd_cryptopunks_floor_price_eth = 'JPEGd Cryptopunks Floor Price ETH' %}
{% set jpegd_pudgy_penguins_floor_price_eth = 'JPEGd Pudgy Penguins Floor Price ETH' %}
{% set krw_usd = 'KRW / USD' %}
{% set link_usd = 'LINK / USD' %}
{% set lrc_eth = 'LRC / ETH' %}
{% set lusd_usd = 'LUSD / USD' %}
{% set matic_usd = 'MATIC / USD' %}
{% set mavia_usd = 'MAVIA / USD' %}
{% set mayc_floor_price = 'MAYC Floor Price' %}
{% set mim_usd = 'MIM / USD' %}
{% set mkr_usd = 'MKR / USD' %}
{% set metis_healthcheck = 'Metis Healthcheck' %}
{% set moonbirds_floor_price = 'Moonbirds Floor Price' %}
{% set nexus_weth_reserves = 'Nexus wETH Reserves' %}
{% set oeth_eth = 'OETH / ETH' %}
{% set ohmv2_eth = 'OHMv2 / ETH' %}
{% set optimism_healthcheck = 'Optimism Healthcheck' %}
{% set otherdeed_for_otherside_floor_price = 'Otherdeed for Otherside Floor Price' %}
{% set pyusd_usd = 'PYUSD / USD' %}
{% set pudgy_penguins_floor_price = 'Pudgy Penguins Floor Price' %}
{% set rdnt_usd = 'RDNT / USD' %}
{% set reth_eth = 'RETH / ETH' %}
{% set rpl_usd = 'RPL / USD' %}
{% set rseth_eth = 'RSETH / ETH' %}
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
{% set sweth_eth = 'SWETH / ETH' %}
{% set scroll_healthcheck = 'Scroll Healthcheck' %}
{% set swell_eth_por = 'Swell ETH PoR' %}
{% set swell_restaked_eth_por = 'Swell Restaked ETH PoR' %}
{% set synthetix_aggregator_debt_ratio = 'Synthetix Aggregator Debt Ratio' %}
{% set synthetix_aggregator_issued_synths = 'Synthetix Aggregator Issued Synths' %}
{% set tbtc_usd = 'TBTC / USD' %}
{% set tusd_usd = 'TUSD / USD' %}
{% set tusd_reserves = 'TUSD Reserves' %}
{% set total_marketcap_usd = 'Total Marketcap / USD' %}
{% set uni_usd = 'UNI / USD' %}
{% set usdc_usd = 'USDC / USD' %}
{% set usdc_eth = 'USDC / ETH' %}
{% set usdd_usd = 'USDD / USD' %}
{% set usdp_usd = 'USDP / USD' %}
{% set usdv_usd = 'USDV / USD' %}
{% set usde_usd = 'USDe / USD' %}
{% set ust_eth = 'UST / ETH' %}
{% set ust_usd = 'UST / USD' %}
{% set veefriends_floor_price = 'VeeFriends Floor Price' %}
{% set weeth_eth = 'weETH / ETH' %}
{% set wbtc_btc = 'WBTC / BTC' %}
{% set wbtc_por = 'WBTC PoR' %}
{% set wing_usd = 'WING / USD' %}
{% set world_of_women_floor_price = 'World of Women Floor Price' %}
{% set xcn_usd = 'XCN / USD' %}
{% set yfi_usd = 'YFI / USD' %}
{% set zrx_usd = 'ZRX / USD' %}
{% set ezeth_eth = 'ezETH / ETH' %}
{% set weeth_eth = 'weETH / ETH' %}

SELECT
   'ethereum' as blockchain,
   feed_name,
   CAST(decimals AS BIGINT) as decimals,
   proxy_address,
   aggregator_address
FROM (values
  ('{{aave_usd}}', 8, 0x547a514d5e3769680Ce22B2361c10Ea13619e8a9, 0x8116B273cD75d79C382aFacc706659DEd5E0a59d),
  ('{{ageur_eur}}', 18, 0xb4d5289C58CE36080b0748B47F859D8F50dFAACb, 0xCADB2f2E0eD54B92D105095499B6b753ec0A5C17),
  ('{{alcx_eth}}', 18, 0x194a9AaF2e0b67c35915cD01101585A33Fe25CAa, 0x74263dB73076C1389d12e5F8ff0E6a72AE86CA24),
  ('{{ankr_usd}}', 8, 0x7eed379bf00005CfeD29feD4009669dE9Bcc21ce, 0xBCFEbD504ec678b9316842C01CA295a03eC2bC85),
  ('{{ape_eth}}', 18, 0xc7de7f4d4C9c991fF62a07D18b3E31e349833A18, 0x72002129A3834d63C57d157DDF069deE37b08F24),
  ('{{ape_usd}}', 8, 0xD10aBbC76679a20055E167BB80A24ac851b37056, 0x14C3da2f2e6Ca4FC76408156A8F43d2975c74de8),
  ('{{ape_usd}}', 8, 0xD10aBbC76679a20055E167BB80A24ac851b37056, 0xa99999b1475F24037e8b6947aBBC7710676E77dd),
  ('{{apy_tvl}}', 8, 0xDb299D394817D8e7bBe297E84AFfF7106CF92F5f, 0x953DA51613067981ff15695695994DD8B1310F6d),
  ('{{arb_usd}}', 8, 0x31697852a68433DbCc2Ff612c516d69E3D9bd08F, 0xDb4fEDd5b8FD533f18034610C207712Ce5dcfbfd),
  ('{{arkb_reserves}}', 18, 0x80f8D7b4fB192De43Ed6aE0DD4A42A60f43641b0, 0xc1D5A7AD2094F34d4C587D77926176A660B4f51f),
  ('{{azuki_floor_price_eth}}', 18, 0xA8B9A447C73191744D5B79BcE864F343455E1150, 0xF0c3668756b9d9590B334768640FC5ACA02aE739),
  ('{{bal_usd}}', 8, 0xdF2917806E30300537aEB49A7663062F4d1F2b5F, 0xbd9350a3a2fd6e3Ad0a053a567f2609a1bf6c505),
  ('{{bat_eth}}', 18, 0x0d16d4528239e9ee52fa531af613AcdB23D88c94, 0x821f24DAcA9Ad4910c1EdE316D2713fC923Da698),
  ('{{bat_usd}}', 8, 0x9441D7556e7820B5ca42082cfa99487D56AcA958, 0x98E3F1BE8E0609Ac8a7681f23e15B696F8e8204d),
  ('{{beanz_official_floor_price}}', 18, 0xA97477aB5ab6ED2f6A2B5Cbe59D71e88ad334b90, 0x844962E9c0D7033a1EC9d5931bA8DC9dED265a2B),
  ('{{bnb_usd}}', 8, 0x14e613AC84a31f709eadbdF89C6CC390fDc9540A, 0xC45eBD0F901bA6B2B8C7e70b717778f055eF5E6D),
  ('{{btc_usd}}', 8, 0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c, 0xdBe1941BFbe4410D6865b9b7078e0b49af144D2d),
  ('{{base_healthcheck}}', 0, 0x48D9DA600EC48DDd6ce7FC1D47D683818e511c81, 0x228e76Eee56FCdAb9D4D95D0E7Ae1E6Db5E3587a),
  ('{{bored_ape_yacht_club_floor_price_eth}}', 18, 0x352f2Bc3039429fC2fe62004a1575aE74001CfcE, 0x6DBD8100fBbfF754831Aa90A53c466d431651885),
  ('{{c3m_eur}}', 8, 0xD41390267Afec3fA5b4c0B3aA6c706556CCE75ec, 0x2B448FE1B7C7A9f2E42F819943e6E6066bb4Ed77),
  ('{{cake_usd}}', 8, 0xEb0adf5C06861d6c07174288ce4D0a8128164003, 0x1C026C25271c1bFbA95B65c848F734a23eA62D4e),
  ('{{cbeth_eth}}', 18, 0xF017fcB346A1885194689bA23Eff2fE6fA5C483b, 0xd74FF3f1b565597E59D44320F53a5C5c8BA85f7b),
  ('{{comp_usd}}', 8, 0xdbd020CAeF83eFd542f4De03e3cF0C28A4428bd5, 0x64d2E1F01A19762dDEE27b1062CC092B66Ff9652),
  ('{{crvusd_usd}}', 8, 0xEEf0C605546958c1f899b6fB336C20671f9cD49F, 0x145f040dbCDFf4cBe8dEBBd58861296012fCB269),
  ('{{cspx_usd}}', 8, 0xF4E1B57FB228879D057ac5AE33973e8C53e4A0e0, 0x9aB931c33E0a21689A823d60e625B57EF1faa9C8),
  ('{{cvx_eth}}', 18, 0xC9CbF687f43176B302F03f5e58470b77D07c61c6, 0xf1F7F7BFCc5E9D6BB8D9617756beC06A5Cbe1a49),
  ('{{cvx_usd}}', 8, 0xd962fC30A72A84cE50161031391756Bf2876Af5D, 0x8d73Ac44Bf11CadCDc050BB2BcCaE8c519555f1a),
  ('{{cachegold_por_usd}}', 8, 0x5586bF404C7A22A4a4077401272cE5945f80189C, 0x6CeA38508B186DE36AAfd0f3B513E708691bc0C4),
  ('{{calculated_xsushi_eth}}', 18, 0xF05D9B6C08757EAcb1fbec18e36A1B7566a13DEB, 0xdEaa4288c85e7e0be40BCE49E76D4e321d20fC36),
  ('{{clonex_floor_price}}', 18, 0x021264d59DAbD26E7506Ee7278407891Bb8CDCCc, 0xB187B5A5A4B0A2Ae32FaEDf0FE4845203E0B7b11),
  ('{{consumer_price_index}}', 18, 0x9a51192e065ECC6BDEafE5e194ce54702DE4f1f5, 0x5a0efD6D1a058A46D3Ac4511861adB8F3540BD49),
  ('{{coolcats_floor_price}}', 18, 0xF49f8F5b931B0e4B4246E4CcA7cD2083997Aa83d, 0xaBd6dc0E14bdC628E62Cc946897C7fEfDCDdcD10),
  ('{{cryptopunks_floor_price_eth}}', 18, 0x01B6710B01cF3dd8Ae64243097d91aFb03728Fdd, 0xF0c85c0F7dC37e1605a0Db446a2A0e33Df7a3358),
  ('{{cryptoadz_floor_price}}', 18, 0xFaA8F6073845DBe5627dAA3208F78A3043F99bcA, 0xc609c4fADdA31980769c9C6716F438f0a6059547),
  ('{{dai_usd}}', 8, 0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9, 0x478238a1c8B862498c74D0647329Aef9ea6819Ed),
  ('{{dpi_eth}}', 18, 0x029849bbc0b1d93b85a8b6190e979fd38F5760E2, 0x36e4f71440EdF512EB410231e75B9281d4FcFC4c),
  ('{{dpi_usd}}', 8, 0xD2A593BF7594aCE1faD597adb697b5645d5edDB2, 0xA122f84935477142295F7451513e502D49316285),
  ('{{dpi_usd}}', 8, 0xD2A593BF7594aCE1faD597adb697b5645d5edDB2, 0x68f1B8317C19ff02fb68A8476C1D3f9Fc5139c0A),
  ('{{doodles_floor_price}}', 18, 0x027828052840a43Cc2D0187BcfA6e3D6AcE60336, 0x440C8fc45C7f00E09c2F437b95FC123888a3d951),
  ('{{ens_usd}}', 8, 0x5C00128d4d1c2F4f652C267d7bcdD7aC99C16E16, 0x780f1bD91a5a22Ede36d4B2b2c0EcCB9b1726a28),
  ('{{eth_btc}}', 8, 0xAc559F25B1619171CbC396a50854A3240b6A4e99, 0x0f00392FcB466c0E4E4310d81b941e07B4d5a079),
  ('{{eth_usd}}', 8, 0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419, 0xE62B71cf983019BFf55bC83B48601ce8419650CC),
  ('{{ethx_eth}}', 18, 0xC5f8c4aB091Be1A899214c0C3636ca33DcA0C547, 0xaA745106Db818BfecC39250260df4d453498279F),
  ('{{eurr_reserves}}', 6, 0x652Ac4468688f277fB84b26940e736a20A87Ac2d, 0x3847bffbC555BcCb482373AD7b779D6b63d7f3CE),
  ('{{fdusd_usd}}', 8, 0xfAA9147190c2C2cc5B8387B4f49016bDB3380572, 0xd57a242FB40ED4526083B6fA05238B3d57f78D45),
  ('{{fil_eth}}', 18, 0x0606Be69451B1C9861Ac6b3626b99093b713E801, 0x9965AD91B4877d29c246445011Ce370b3890C5C2),
  ('{{frax_eth}}', 18, 0x14d04Fff8D21bd62987a5cE9ce543d2F1edF5D3E, 0x56f98706C14DF5C290b02Cec491bB4c20834Bb51),
  ('{{fast_gas_gwei}}', 0, 0x169E633A2D1E6c10dD91238Ba11c4A708dfEF37C, 0x785433d8b06D77D68dF6be63944742130A4530d1),
  ('{{gbpt_por}}', 18, 0xF6f5b570aB6E39E55558AfD8E1E30c5f20E6527E, 0x0a0aba8efAB65fDD3fa7e6afcb8128bCd6ffbdBF),
  ('{{gho_usd}}', 8, 0x3f12643D3f6f874d39C2a4c9f2Cd6f2DbAC877FC, 0x2E1D7e5Ba9A04ff2AA15be73b812fe1F8A43c3d7),
  ('{{grt_eth}}', 18, 0x17D054eCac33D91F7340645341eFB5DE9009F1C1, 0x7531f77095Bed9d63cB3E9EA305111a7DCE969A2),
  ('{{grt_eth}}', 18, 0x17D054eCac33D91F7340645341eFB5DE9009F1C1, 0x91401cedCBFd9680cE193A5F54E716504233e998),
  ('{{hbtc_por}}', 18, 0x0A8cD0115B1EE87EbA5b8E06A9a15ED93e230f7a, 0x6aa553CB870a54BD62bb54E11f0B2c919925E726),
  ('{{high_usd}}', 8, 0x5C8D8AaB4ffa4652753Df94f299330Bb4479bF85, 0xbD05823Efac9A1CcC612c00A6bf51Cc84930126a),
  ('{{ib01_usd}}', 8, 0x32d1463EB53b73C095625719Afa544D5426354cB, 0x5EE6Ee50c1cB3E8Da20eE83D57818184387433e8),
  ('{{ibta_usd}}', 8, 0xd27e6D02b72eB6FCe04Ad5690C419196B4EF2885, 0x5f8c943a29FFfC7Df8cE4001Cf1bedbCFC610476),
  ('{{idr_usd}}', 8, 0x91b99C9b75aF469a71eE1AB528e8da994A5D7030, 0x156710f56dC5F0C022505A9ffE95b0b51A7c5c9A),
  ('{{imx_usd}}', 8, 0xBAEbEFc1D023c0feCcc047Bff42E75F15Ff213E6, 0x3f00247Dc3bc14A8dCfA682318Ce566b1f34343A),
  ('{{jpegd_cryptopunks_floor_price_eth}}', 18, 0x35f08E1b5a18F1F085AA092aAed10EDd47457484, 0x3D1fDFB6C9579D249d2bA6D85043C53Cac77fB3a),
  ('{{jpegd_pudgy_penguins_floor_price_eth}}', 18, 0xaC9962D846D431254C7B3Da3AA12519a1E2Eb5e7, 0xbFbCc713B8320D924079eff26fcC773353275F10),
  ('{{krw_usd}}', 8, 0x01435677FB11763550905594A16B645847C1d0F3, 0x86e345D4113E1105053A81240C75b56B437dA6Ef),
  ('{{link_usd}}', 8, 0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c, 0x20807Cf61AD17c31837776fA39847A2Fa1839E81),
  ('{{lrc_eth}}', 18, 0x160AC928A16C93eD4895C2De6f81ECcE9a7eB7b4, 0x9405e02996Aa6f2176E2748EEfbCedd405870cee),
  ('{{lusd_usd}}', 8, 0x3D7aE7E594f2f2091Ad8798313450130d0Aba3a0, 0x27b97a63091d185cE056e1747624b9B92BAAD056),
  ('{{matic_usd}}', 8, 0x7bAC85A8a13A4BcD8abb3eB7d6b4d632c5a57676, 0x4B35F7854e1fd8291f4EC714aC3EBB1DeA450585),
  ('{{mavia_usd}}', 8, 0x29d26C008e8f201eD0D864b1Fd9392D29d0C8e96, 0x8F8fb37D82cB065A0fFe96D8e886717C838C9668),
  ('{{mayc_floor_price}}', 18, 0x1823C89715Fe3fB96A24d11c917aCA918894A090, 0xb17Eac46CF1B9C5fe2F707c8A47AFc4d208b3E83),
  ('{{mim_usd}}', 8, 0x7A364e8770418566e3eb2001A96116E6138Eb32F, 0x18f0112E30769961AF90FDEe0D1c6B27E6d72D92),
  ('{{mkr_usd}}', 8, 0xec1D1B3b0443256cc3860e24a46F108e699484Aa, 0x71Febc2F741F113af322e1B576eF005A4424574F),
  ('{{metis_healthcheck}}', 8, 0x3425455fe737cdaE8564640df27bbF2eCD56E584, 0x31c8b5A8F0d286a4Bfcf669E18393b18E22B140D),
  ('{{moonbirds_floor_price}}', 18, 0x9cd36E0E8D3C27d630D00406ACFC3463154951Af, 0x8d0003e5c1C8EB67e04023a21291cf01CFd2E4a1),
  ('{{nexus_weth_reserves}}', 18, 0xCc72039A141c6e34a779eF93AEF5eB4C82A893c7, 0xCA71bBe491079E138927f3f0AB448Ae8782d1DCa),
  ('{{oeth_eth}}', 18, 0x703118C4CbccCBF2AB31913e0f8075fbbb15f563, 0xAA2794B0b931966B88c2DABBE3Ac70B9c1521f4a),
  ('{{ohmv2_eth}}', 18, 0x9a72298ae3886221820B1c878d12D872087D3a23, 0x9Aae856973A0Cafa084b82F7BC4C6C2893A9139b),
  ('{{optimism_healthcheck}}', 8, 0x59c2287c8E848310c809C061a1Be0d1556eFF4e2, 0x90f07EDF949673732178D9F305B8183524120ea8),
  ('{{otherdeed_for_otherside_floor_price}}', 18, 0x6e3A4376B4C8D3ba49602f8542D9D3C4A87ba901, 0xE308e892e153B899404928b6C705b7c8Da231F0F),
  ('{{pyusd_usd}}', 8, 0x8f1dF6D7F2db73eECE86a18b4381F4707b918FB1, 0x60128Ad1eC1A26e338054c7C763b170351355FBD),
  ('{{pudgy_penguins_floor_price}}', 18, 0x9f2ba149c2A0Ee76043d83558C4E79E9F3E5731B, 0x1A93f0C2168DfeEF0801D85E74FB21F4534Ddfc8),
  ('{{rdnt_usd}}', 8, 0x393CC05baD439c9B36489384F11487d9C8410471, 0x69Ad141613e9A3df3cEB3541884F71B36A25Db3A),
  ('{{reth_eth}}', 18, 0x536218f9E9Eb48863970252233c8F271f554C2d0, 0x9cB248E68fb81d0CFE7D6B3265Fe6Bf123A71FE0),
  ('{{rpl_usd}}', 8, 0x4E155eD98aFE9034b7A5962f6C84c86d869daA9d, 0x5Df960959De45A2BA9DC11e6fD6F77107F43256C),
  ('{{rseth_eth}}', 18, 0x03c68933f7a3F76875C0bc670a58e69294cDFD01, 0xeA7660bC11b9cE10E127f13375C54f64BeB17dB4),
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
  ('{{sweth_eth}}', 18, 0xec21B3e882CE09928cb397DcfF31B15cBBD1e1C3, 0xeE8AEe6E5Cb9D827C728D1BE1729b6F56A5fA18a),
  ('{{scroll_healthcheck}}', 0, 0x7fb9B4a05e7B4F0c1Ac0B0046784cc0aCE8CbBC5, 0x9195bddFe7E393702C332F1b9B590ec49EB12060),
  ('{{swell_eth_por}}', 18, 0x60cbE8D88EF519cF3C62414D76f50818D211fea1, 0x477716B8e95749bF31ce26cF4e4E4Af87b8Acf59),
  ('{{swell_restaked_eth_por}}', 18, 0x0c89c488e763AC2d69cB058CCAC7A8B283EE3DbA, 0xEe84aAFa604a00ef8a1eEA5152c9a1500D38BEE5),
  ('{{synthetix_aggregator_debt_ratio}}', 27, 0x0981af0C002345c9C5AD5efd26242D0cBe5aCA99, 0xc7BB32a4951600FBac701589C73e219b26Ca2DFC),
  ('{{synthetix_aggregator_issued_synths}}', 18, 0xbCF5792575bA3A875D8C406F4E7270f51a902539, 0x59CCf62B862f99B5aEd8857FBAdB7F895f6c59D5),
  ('{{tbtc_usd}}', 8, 0x8350b7De6a6a2C1368E7D4Bd968190e13E354297, 0x0A7AaAa55cEe361EBE1d57F80345285dbAF96FCC),
  ('{{tusd_usd}}', 8, 0xec746eCF986E2927Abd291a2A1716c940100f8Ba, 0x98953e9C76573e06ec265Bdde1dbB89fa02d56d3),
  ('{{tusd_reserves}}', 18, 0xBE456fd14720C3aCCc30A2013Bffd782c9Cb75D5, 0xAC099D59755982757537F13c7c4Ae8c8d9F030B9),
  ('{{total_marketcap_usd}}', 8, 0xEC8761a0A73c34329CA5B1D3Dc7eD07F30e836e2, 0x9257D83A0DdA413cA24F66dD32A056Bc2eBAFd2e),
  ('{{uni_usd}}', 8, 0x553303d460EE0afB37EdFf9bE42922D8FF63220e, 0x373BCe97bec13BfA8A5f07Cc578EC2D77f80c589),
  ('{{usdc_eth}}', 18, 0x986b5E1e1755e3C2440e960477f25201B0a8bbD4, 0xe5BbBdb2Bb953371841318E1Edfbf727447CeF2E),
  ('{{usdc_usd}}', 8, 0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6, 0x789190466E21a8b78b8027866CBBDc151542A26C),
  ('{{usdd_usd}}', 8, 0x0ed39A19D2a68b722408d84e4d970827f61E6c0A, 0x589a85FC02EB6bB86D1c84c1a75abbB012C661De),
  ('{{usdp_usd}}', 8, 0x09023c0DA49Aaf8fc3fA3ADF34C6A7016D38D5e3, 0xF3d70857B489Ecc6768D0982B773E1Cba9E1f00b),
  ('{{usdv_usd}}', 8, 0x925B831EB4c9fFA7e384254fb2cd508c65FAe3FE, 0xbf35cFdd68de8b07eA8a3C7a34117557F64050D1),
  ('{{usde_usd}}', 8, 0xa569d910839Ae8865Da8F8e70FfFb0cBA869F961, 0xB735cC58d71dEAc4cfC46dE68d3b04988F7D7b2d),
  ('{{ust_eth}}', 18, 0xa20623070413d42a5C01Db2c8111640DD7A5A03a, 0x4a81f77C8BBcA2CbA8110279cDbC9F1A8D3eAE6B),
  ('{{ust_usd}}', 8, 0x8b6d9085f310396C6E4f0012783E9f850eaa8a82, 0x01b87e7fF78022A70394d3C6Dd127D0c709e3beA),
  ('{{ust_usd}}', 8, 0x8b6d9085f310396C6E4f0012783E9f850eaa8a82, 0x5EDd5F803b831b47715aD3e11a90dD244F0cD0a9),
  ('{{veefriends_floor_price}}', 18, 0x35bf6767577091E7f04707c0290b3f889e968307, 0xe0552DC960366F67Da00CB3d9DF441F24B5C2AC1),
  ('{{weeth_eth}}', 18, 0x5c9C449BbC9a6075A2c061dF312a35fd1E05fF22, 0x4dF36F726d8059d881294166dB52c1D13e976FE7),
  ('{{wbtc_btc}}', 8, 0xfdFD9C85aD200c506Cf9e21F1FD8dd01932FBB23, 0xD7623f1d24b35c392862fB67C9716564A117C9DE),
  ('{{wbtc_por}}', 8, 0xa81FE04086865e63E12dD3776978E49DEEa2ea4e, 0xB622b7D6d9131cF6A1230EBa91E5da58dbea6F59),
  ('{{wing_usd}}', 8, 0x134fE0a225Fb8e6683617C13cEB6B3319fB4fb82, 0xc29d104A418a08407f9f2CDb614c1CDCf82986e0),
  ('{{wing_usd}}', 8, 0x134fE0a225Fb8e6683617C13cEB6B3319fB4fb82, 0x2c9A8c2caEb80FEb24048587a10BFB6aeFF601c5),
  ('{{world_of_women_floor_price}}', 18, 0xDdf0B85C600DAF9e308AFed9F597ACA212354764, 0x45B68d24Df514BF13a838d88bE4363F8011719de),
  ('{{xcn_usd}}', 8, 0xeb988B77b94C186053282BfcD8B7ED55142D3cAB, 0xD6A3a9Bb4bd49DdB2374CA58Edf47a8bB63Af3d2),
  ('{{yfi_usd}}', 8, 0xA027702dbb89fbd58938e4324ac03B58d812b0E1, 0xcac109af977AC94929A5dD37ed8Af763BAD78151),
  ('{{zrx_usd}}', 8, 0x2885d15b8Af22648b98B122b22FDF4D2a56c6023, 0x4Dde220fF2690A350b0Ea9404F35C8f3Ad012584),
  ('{{ezeth_eth}}', 18, 0x636A000262F6aA9e1F094ABF0aD8f645C44f641C, 0x85Fbd46EDeD893392e52A02BC5ac0294FB06f88D),
  ('{{weeth_eth}}', 18, 0x5c9C449BbC9a6075A2c061dF312a35fd1E05fF22, 0x4dF36F726d8059d881294166dB52c1D13e976FE7)
) a (feed_name, decimals, proxy_address, aggregator_address)
