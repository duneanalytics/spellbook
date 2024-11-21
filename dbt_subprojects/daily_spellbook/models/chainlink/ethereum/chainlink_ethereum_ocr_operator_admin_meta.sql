{{
  config(
    
    alias='ocr_operator_admin_meta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan", "linkpool_jon"]\') }}'
  )
}}

{% set a01node = '01Node' %}
{% set tsystems = 'Deutsche Telekom MMS' %}
{% set alphachain = 'Alpha Chain' %}
{% set artifact = 'Artifact' %}
{% set bharvest = 'B Harvest' %}
{% set blockdaemon = 'Blockdaemon' %}
{% set blocksizecapital = 'Blocksize Capital' %}
{% set certusone = 'Certus One' %}
{% set chainlayer = 'Chainlayer' %}
{% set chainlink = 'Chainlink' %}
{% set chorusone = 'Chorus One' %}
{% set coinbase = 'Coinbase' %}
{% set cosmostation = 'Cosmostation' %}
{% set cryptomanufaktur = 'CryptoManufaktur' %}
{% set dmakers = 'dMakers' %}
{% set dextrac = 'DexTrac' %}
{% set dxfeed = 'dxFeed' %}
{% set easy2stake = 'Easy 2 stake' %}
{% set everstake = 'Everstake' %}
{% set fiews = 'Fiews' %}
{% set figmentnetworks = 'Figment Networks' %}
{% set frameworkventures = 'Framework Ventures' %}
{% set honeycomb = 'Honeycomb.market' %}
{% set huobi = 'Huobi' %}
{% set infinitystones = 'Infinity Stones' %}
{% set infura = 'Infura' %}
{% set inotel = 'Inotel' %}
{% set kaiko = 'Kaiko' %}
{% set kyber = 'Kyber' %}
{% set kytzu = 'Kytzu' %}
{% set lexisnexis = 'LexisNexis' %}
{% set linkforest = 'LinkForest' %}
{% set linkpool = 'LinkPool' %}
{% set linkriver = 'LinkRiver' %}
{% set matrixedlink = 'Matrixed.Link' %}
{% set newroad = 'Newroad Network' %}
{% set nomics = 'Nomics.com' %}
{% set northwestnodes = 'NorthWest Nodes' %}
{% set omniscience = 'Omniscience' %}
{% set onchaintech = 'On-chain Tech' %}
{% set orionmoney = 'Orion.Money' %}
{% set p2porg = 'P2P.org' %}
{% set paradigm = 'Paradigm Citadel' %}
{% set piertwo = 'Pier Two' %}
{% set prophet = 'Prophet' %}
{% set rhino = 'RHINO' %}
{% set simplyvc = 'Simply VC' %}
{% set snzpool = 'SNZPool' %}
{% set stakefish = 'stake.fish' %}
{% set stakesystems = 'Stake Systems' %}
{% set staked = 'Staked' %}
{% set stakin = 'Stakin' %}
{% set stakingfacilities = 'Staking Facilities' %}
{% set swisscom = 'Swisscom' %}
{% set syncnode = 'SyncNode' %}
{% set thenetworkfirm = 'The Network Firm' %}
{% set tiingo = 'Tiingo' %}
{% set validationcloud = 'Validation Cloud' %}
{% set vulcan = 'Vulcan Link' %}
{% set wetez = 'Wetez' %}
{% set xbto = 'XBTO' %}
{% set youbi = 'Youbi' %}
{% set ztake = 'Ztake.org' %}

SELECT admin_address, operator_name FROM (VALUES
  (0x7A30E4B6307c0Db7AeF247A656b44d888B23a2DC, '{{a01node}}'),
  (0xD9459cc85E78e0336aDb349EAbF257Dbaf9d5a2B, '{{a01node}}'),
  (0x89177B9c203bA0A9294aecf2f3806d98907bec6f, '{{tsystems}}'),
  (0xa5D0084A766203b463b3164DFc49D91509C12daB, '{{alphachain}}'),
  (0xfA3430d84324ABC9ac8AAf30B2D26260F5172ad0, '{{alphachain}}'),
  (0xba8Bcb4EB9a90D5A0eAe0098496703b49f909cB2, '{{artifact}}'),
  (0x6cDC3Efa3bAa392fAF3E5c1Ca802E15B6185E0e8, '{{bharvest}}'),
  (0x3615Fa045f00ae0eD60Dc0141911757c2AdC5E03, '{{blockdaemon}}'),
  (0x7CC60c9C24E9A290Db55b1017AF477E5c87a7550, '{{blocksizecapital}}'),
  (0xdF0df748c782f0B9A52aEcb223Bf60e23f261283, '{{certusone}}'),
  (0x8D689476EB446a1FB0065bFFAc32398Ed7F89165, '{{certusone}}'),
  (0x9D219125a0CE10241b4eC1280c2F880475f172f1, '{{chainlayer}}'),
  (0x56aCCE2EE3f86c0057C4ddfa7Bba1C8D99c83565, '{{chainlink}}'),
  (0x304D69727DD28ad6E1aa2c01Db301dB556C7b725, '{{chainlink}}'),
  (0x29fC5aACd613410b68c9c08d4e1656e3c890E482, '{{chorusone}}'),
  (0xb44A46a7B245D82e15F07Cb352Fd5f1d3dBF65F6, '{{coinbase}}'),
  (0x1B17eB8FAE3C28CB2463235F9D407b527ba4e6Dd, '{{cosmostation}}'),
  (0x59eCf48345A221E0731E785ED79eD40d0A94E2A5, '{{cryptomanufaktur}}'),
  (0x3b74c27115965ba74D695E3AEdb615F991F3f310, '{{dmakers}}'),
  (0x9efa0A617C0552F1558c95993aA8b8A68b3e709C, '{{dextrac}}'),
  (0xb284a468522663F6219f2912ca10145B52b13503, '{{dxfeed}}'),
  (0x991812566f6E14897Fc1e401D24de19845c0442f, '{{dxfeed}}'),
  (0xFdC770353dC0bFCE80a17Ab8a6a2E7d80590f1Ba, '{{easy2stake}}'),
  (0x039fDFDb14911608a34eBCDa6009a80EF5D16e50, '{{everstake}}'),
  (0x15918ff7f6C44592C81d999B442956B07D26CC44, '{{fiews}}'),
  (0x95c98112bd9635A3159518401Ae227D5a296e994, '{{figmentnetworks}}'),
  (0x6eF38c3d1D85B710A9e160aD41B912Cb8CAc2589, '{{frameworkventures}}'),
  (0x47adCDcaA250C257C6e4db6dD091C6A6739333C9, '{{honeycomb}}'),
  (0xC65c57A04e2cD361E7049f6c182ce1b62c7A92b3, '{{huobi}}'),
  (0xDFBfB73f3013bc1584CcAa0CD2D9621194aEd29B, '{{infinitystones}}'),
  (0xe6B12850b3979C50d221fb84d40FB94AeFBaB867, '{{infura}}'),
  (0xdD831352762e9de7ad5a264990e1bB9F87A6Fc21, '{{inotel}}'),
  (0xB8C6E43f37E04A2411562a13c1C48B3ad5975cf4, '{{inotel}}'),
  (0xaA71518e7895C933f60EB2F02359cC40Ad6ef670, '{{kaiko}}'),
  (0xAb5176dd2891dC01f5f8EC786263d85ee0690eC2, '{{kaiko}}'),
  (0x54919167e0389b07a99e7cE9F66F1fd9f8C75d77, '{{kyber}}'),
  (0x001E0d294383d5b4136476648aCc8D04a6461Ae3, '{{kytzu}}'),
  (0x57F7f85C151A8A96CC20fEa6a43622334C335fe4, '{{kytzu}}'),
  (0x098a4C7ceCbfb8534e5Ab3f9c8F6C87845Fc5109, '{{lexisnexis}}'),
  (0x4564A9c6061f6f1F2Eadb954B1b3C241D2DC984e, '{{linkforest}}'),
  (0x69f0fB5f300C45AfEbBBCd85E930EDBB142c0D48, '{{linkforest}}'),
  (0xD48fc6E2B73C2988fA50C994181C0CdCa850D62a, '{{linkforest}}'),
  (0x797de2909991C66C66D8e730C8385bbab8D18eA6, '{{linkpool}}'),
  (0xCa878CF4a27690637c07B39ae06D26f7679Be4FC, '{{linkpool}}'),
  (0xDF812B91D8bf6DF698BFD1D8047839479Ba63420, '{{linkpool}}'),
  (0xe9E11963f61322299f9919ff1dda01a825E82dBC, '{{linkpool}}'),
  (0x14f94049397C3F1807c45B6f854Cb5F36bC4393B, '{{linkriver}}'),
  (0x4dc81f63CB356c1420D4620414f366794072A3a8, '{{matrixedlink}}'),
  (0xAB35418fB9f8B13E3e6857c36A0769b9F94a87EC, '{{newroad}}'),
  (0x425f682362b2b1032A212226a474Fa7D3703f2a8, '{{nomics}}'),
  (0x0921E157b690c4F89F7C2a210cFd8bF3964F6776, '{{northwestnodes}}'),
  (0x47044eE2F23001F8a03FB2f7d2ce6645aDA4D12A, '{{omniscience}}'),
  (0x70Ba986EF5a805A7019415eDF342eD4365331fF1, '{{omniscience}}'),
  (0x35DaC078fC9E6e45d89a6CBc78A776BA719b485D, '{{onchaintech}}'),
  (0xE2063AA95B35f8121A5E2f58BfE6a985270ABA77, '{{orionmoney}}'),
  (0xa0181758B14EfB2DAdfec66d58251Ae631e2B942, '{{orionmoney}}'),
  (0xCDa423ee5A7A886eF113b181469581306fC8B607, '{{p2porg}}'),
  (0xB45A43e998286ab3Be4106b4c381f01dccE772a4, '{{p2porg}}'),
  (0xfb390441fF968F7569cd6F3CF01cb7214DFeed31, '{{paradigm}}'),
  (0x3FB4600736d306Ee2A89EdF0356D4272fb095768, '{{piertwo}}'),
  (0xBDB624CD1051F687f116bB0c642330B2aBdfcc06, '{{prophet}}'),
  (0xDA80050Ed4F50033949608208f79EE43Ab91dF55, '{{rhino}}'),
  (0x4fBefaf1BFf0130945C61603B97D38DD6e21f5Cf, '{{simplyvc}}'),
  (0x9cCbFD17FA284f36c2ff503546160B256d1CD3D1, '{{snzpool}}'),
  (0x21F1dB6E4B5e1dF5c68bC1dfB58c28942Ed4737D, '{{stakefish}}'),
  (0x61C808D82A3Ac53231750daDc13c777b59310bD9, '{{stakefish}}'),
  (0xA68F8E34B775aA1f0c0E9028b13BdB481eCf486c, '{{stakesystems}}'),
  (0x87BCD34B45784081fda93a10797e2dB51B8466Aa, '{{staked}}'),
  (0x5823D12aD85Ef11f4e5508AB3559a37D87CF507A, '{{stakin}}'),
  (0xdD4Bc51496dc93A0c47008E820e0d80745476f22, '{{stakingfacilities}}'),
  (0xcc3F2FB70B5941FBdB9F97fD6Df6997A01229ecE, '{{swisscom}}'),
  (0xC51D3470693BC049809A1c515606124c7C75908d, '{{syncnode}}'),
  (0x7c9998a91AEA813Ea8340b47B27259D74896d136, '{{thenetworkfirm}}'),
  (0xfAE26207ab74ee528214ee92f94427f8Cdbb6A32, '{{tiingo}}'),
  (0x183A96629fF566e7AA8AfA38980Cd037EB40A59A, '{{validationcloud}}'),
  (0x7D0f8dd25135047967bA6C50309b567957dd52c3, '{{vulcan}}'),
  (0x4E28977d71f148ae2c523e8Aa4b6F3071d81Add1, '{{vulcan}}'),
  (0x111f1B41f702c20707686769a4b7f25c56C533B2, '{{wetez}}'),
  (0x0b16EC1044F60F03B0e815f863bd4d27638cbD0A, '{{xbto}}'),
  (0x3331452b9D6f76E35951f3B8C5881D5801f08612, '{{youbi}}'),
  (0x41EdD305eABFd3497C98341F8D0849F3C520b896, '{{ztake}}'),
  (0xC8c30Fa803833dD1Fd6DBCDd91Ed0b301EFf87cF, '{{ztake}}'),
  (0x9d69B0fcbcf9a7e513E947Cd7ce2019904e2E764, '{{ztake}}')
) AS tmp_node_meta(admin_address, operator_name)
