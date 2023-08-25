{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_operator_node_meta'),
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
{% set prophet = 'Prophet' %}
{% set rhino = 'RHINO' %}
{% set securedatalinks = 'Secure Data Links' %}
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

SELECT node_address, operator_name FROM (VALUES
  (0xCF4Be57aA078Dc7568C631BE7A73adc1cdA992F8, '{{a01node}}'),
  (0x7147333c6d821612577481458E512560bfA12ebD, '{{a01node}}'),
  (0xddEB598fe902A13Cc523aaff5240e9988eDCE170, '{{tsystems}}'),
  (0xA2C13eafA8417d5eE8f1B5D50b99D42CbFe910bA, '{{alphachain}}'),
  (0x5a8216a9c47ee2E8Df1c874252fDEe467215C25b, '{{alphachain}}'),
  (0x165Ff6730D449Af03B4eE1E48122227a3328A1fc, '{{alphachain}}'),
  (0xF585A4aE338bC165D96E8126e8BBcAcAE725d79E, '{{artifact}}'),
  (0xc61a7e5a04A5d32ffe8e01f77Cb39253bf21D2aC, '{{bharvest}}'),
  (0x57CD4848b12469618b689163f507817940AccA02, '{{blockdaemon}}'),
  (0x47b9161Daf189017BF1b499455c65F9234DF3FA3, '{{blockdaemon}}'),
  (0x7663C5790E1eBf04197245d541279D13f3c2f362, '{{blocksizecapital}}'),
  (0x11B6E91E5f8E9f2bC3d09d5c5113134Cc85754a6, '{{certusone}}'),
  (0xc74cE67BfC623c803D48AFc74a09A6FF6b599003, '{{chainlayer}}'),
  (0x18c930d5EA5e33A4b633Cf52d5e83278a6080347, '{{chainlayer}}'),
  (0x6B056837e1968e495d05d7CC0114E9693d2C9002, '{{chainlink}}'),
  (0x64c735D72EAB90C04da523B6b9895773ACb60F5D, '{{chorusone}}'),
  (0xd461DCb175C22b31250f2D21d8034710853547Cb, '{{coinbase}}'),
  (0x70bC61658615725A82D5e78FF0066BFCb1b98988, '{{cosmostation}}'),
  (0x9741569DEDB1E0cB204f2dF7f43f7a52bB49ba3A, '{{cryptomanufaktur}}'),
  (0xCe859E48f6cE9834a119Ba04FdC53c1D4F1082A7, '{{dmakers}}'),
  (0xb976d01275B809333E3EfD76D1d31fE9264466D0, '{{dextrac}}'),
  (0x1e1956cAfdB99f8A757EF902B2A4C67F3122ffcc, '{{dextrac}}'),
  (0x620249ff124b6c62FeA908C27292461ceB9874B2, '{{dxfeed}}'),
  (0xf6BfDBAFf15E778CbaA0C71CDb752810868a176f, '{{dxfeed}}'),
  (0x5565b5362FF9f468bA2f144f38b87187C9a010A8, '{{easy2stake}}'),
  (0x66E3dCA826b04B5d4988F7a37c91c9b1041e579D, '{{easy2stake}}'),
  (0xa938d77590aF1d98BaB7dc4a0bde594fC3F9c403, '{{everstake}}'),
  (0x218B5a7861dBf368D09A84E0dBfF6C6DDbf99DB8, '{{fiews}}'),
  (0xd156C977cB92Fc00E3A44f7856Db3fCc9a0f28A7, '{{fiews}}'),
  (0xEdBED9F5dEA03dD0ec484577C41502af68B7c46a, '{{figmentnetworks}}'),
  (0x2a4a7afA40a9D03B425752fb4cFd5f0FF5b3964C, '{{frameworkventures}}'),
  (0x6878fb222FfF9A2fE3C0Cde77D281916f8D296b3, '{{huobi}}'),
  (0xe4327d547F8C02e57451b2472B8f9a853D855839, '{{huobi}}'),
  (0x982fa4d5F5C8C0063493AbE58967cA3B7639F10F, '{{infinitystones}}'),
  (0x8C4BC738c709BE322Fe4C078032850Cd10ab0032, '{{infura}}'),
  (0xddA14A7c503341Fc6Fe9C002CA7524bF74ec8918, '{{inotel}}'),
  (0xDbfea8D5822141c13f92CaA06EB94d0F3d67C243, '{{inotel}}'),
  (0x9850E11D2c33B43AB80d478CCC69042b46ab3857, '{{kaiko}}'),
  (0xF42336e35D5C1D1D0DB3140E174BcFc3945f6822, '{{kyber}}'),
  (0xf16e77a989529AA4C58318acEe8A1548Df3fcCc1, '{{kytzu}}'),
  (0xf6E7Dba31369024F0044F24ce5dc2C612B298EDd, '{{lexisnexis}}'),
  (0x3C4ad65F5b4884397e1F09596c7ac7F8F95b3fF3, '{{linkforest}}'),
  (0x3DAbA1A7508c9de6039fCFDA35B5c6D1C103c68f, '{{linkforest}}'),
  (0xcC29be4Ca92D4Ecc43C8451fBA94C200B83991f6, '{{linkpool}}'),
  (0x1589d072aC911a55c2010D97839a1f61b1e3323A, '{{linkpool}}'),
  (0x686beC83b59F8b23A6129f03550A3Aad245a543C, '{{linkriver}}'),
  (0xd0fF3C55A27c930069Cb4EFA32921B89792CA8CC, '{{linkriver}}'),
  (0x1feEc90f63B1927d1078D123A57f940E680a3AbF, '{{matrixedlink}}'),
  (0x9cFAb1513FFA293E7023159B3C7A4C984B6a3480, '{{newroad}}'),
  (0xE3cd128883f2954D78923487B67Ea7C4F25C7C46, '{{nomics}}'),
  (0x632f869a26Ab4DA58e2Da476EE74800f2BAE060A, '{{northwestnodes}}'),
  (0xF07131F578a5F708AE2CCB9faF98458099E0FFB4, '{{omniscience}}'),
  (0xB5b90F596341127dE460465dEA9b28b8a6Bd1984, '{{onchaintech}}'),
  (0x0D785c33bCe2D09e521BFc433efe42Da53d3A898, '{{orionmoney}}'),
  (0x8F3Ab0e87B70a57bD4980111a99a1b2c4b8334F4, '{{p2porg}}'),
  (0x23c8Fbd5E14A9565707D8D1a88045F2fA5648968, '{{p2porg}}'),
  (0x8b1d49a93A84B5dA0917a1ed42D8a3E191C28524, '{{prophet}}'),
  (0xF1595809dE873a363d1647853C37dA5506Ed8Da6, '{{prophet}}'),
  (0x634438d879a90a25437B87168252c2b983734391, '{{rhino}}'),
  (0xF7C7AEaECD2349b129d5d15790241c32eeE4607B, '{{securedatalinks}}'),
  (0x61317C73d0225b2E37140fb9664d607B450613C6, '{{simplyvc}}'),
  (0x3E70292211fDe00095408442766C7E56Eb91176c, '{{simplyvc}}'),
  (0x7BFb89db2d7217c57C3Ad3d4B55826eFD17dC2e9, '{{snzpool}}'),
  (0xFa0E4F48a369BB3eCBCEe0B5119379EA8D1bcF29, '{{stakefish}}'),
  (0x9e1735D07F77ABA85415fbE19fdb371a56Cabf07, '{{stakesystems}}'),
  (0xBbf078A8849D74623e36E6DBBdC8e0a35E657C26, '{{staked}}'),
  (0x3aE9D0B74E3968CFCf89A4dE4f0D8B2A326a1Dfd, '{{stakin}}'),
  (0x43793ee58E0a3D920e3e4a115A9FA07dc4B09715, '{{stakingfacilities}}'),
  (0x571476978D2eE5493199A3458Df3aA14afeAdED8, '{{swisscom}}'),
  (0x0312EA121df0a323fF535B753172736cc9d53d13, '{{syncnode}}'),
  (0xa7767CDb3252397d9D6050acD84819AFaBcd2Ff1, '{{syncnode}}'),
  (0xD084c90d0e486ade2c045374dB447b99f94811Ee, '{{thenetworkfirm}}'),
  (0x265b3Aaeb858F32Fe18CFc28EEA21977Fc379F3C, '{{tiingo}}'),
  (0xC4b732Fd121F2f3783A9Ac2a6C62fD535FD13FdA, '{{validationcloud}}'),
  (0x52e77F4356bB39cBA841dC3E9c28eCe86900d68A, '{{validationcloud}}'),
  (0xD22c87Dc7a3F12dcBB75CEbDA2e96f6766AE114F, '{{vulcan}}'),
  (0x5a6fCc02D8c50eA58a22115A7c4608b723030016, '{{wetez}}'),
  (0xe3E0596AC55Ae6044b757baB27426F7dC9e018d4, '{{xbto}}'),
  (0x7744F58E29849Bc7C804e4F4b88d0CE12f068513, '{{youbi}}'),
  (0xDdE59ceeC7A2cC8B2bD78199877BA22018966813, '{{ztake}}')
) AS tmp_node_meta(node_address, operator_name)
