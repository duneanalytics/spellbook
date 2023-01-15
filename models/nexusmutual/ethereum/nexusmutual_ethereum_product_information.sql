{{ config(
    alias ='product_information',
    materialized='view',
    unique_key = ['contract_address', 'product_name', 'product_type', 'syndicate', 'date_added'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["guyhowlett"]\') }}'
    )
}}

SELECT lower('0xF5DCe57282A584D2746FaF1593d3121Fcac444dC') as contract_address, 'Compound Sai' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x8B3d70d628Ebd30D4A2ea82DB95bA2e906c71633') as contract_address, 'bZx v1' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x5504a1d88005236147EC86C62cfb53043bD1276a') as contract_address, 'Unknown' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x4Ddc2D193948926D02f9B1fE9e1daa0718270ED5') as contract_address, 'Compound ETH' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x080bf510FCbF18b91105470639e9561022937712') as contract_address, '0x v2.1' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x16de59092dAE5CcF4A1E6439D611fd0653f0Bd01') as contract_address, 'iearn yDAI v1' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xAF350211414C5DC176421Ea05423F0cC494261fB') as contract_address, 'Saturn DAO Token' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643') as contract_address, 'Compound DAI' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x2157A7894439191e520825fe9399aB8655E0f708') as contract_address, 'Uniswap Exchange Template' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2') as contract_address, 'Maker Token' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x6e95C8E8557AbC08b46F3c347bA06F8dC012763f') as contract_address, 'Legacy Gnosis MultiSig' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xb1CD6e4153B2a390Cf00A6556b0fC1458C4A5533') as contract_address, 'Bancor ETHBNT Token' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x29fe7D60DdF151E5b52e5FAB4f1325da6b2bD958') as contract_address, 'Pool Together DAI' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x4a57E687b9126435a9B19E4A802113e266AdeBde') as contract_address, 'Flexacoin Token' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x519b70055af55A007110B4Ff99b0eA33071c720a') as contract_address, 'dxDAO' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x802275979B020F0ec871c5eC1db6e412b72fF20b') as contract_address, 'Nuo' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xb7896fce748396EcFC240F5a0d3Cc92ca42D7d84') as contract_address, 'Pool Together SAI' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x932773aE4B661029704e731722CF8129e1B32494') as contract_address, 'Pool Together v2' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xB1dD690Cc9AF7BB1a906A9B5A94F94191cc553Ce') as contract_address, 'Argent' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x2b591e99afE9f32eAA6214f7B7629768c40Eeb39') as contract_address, 'Hex Token' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x2C4Bd064b998838076fa341A83d007FC2FA50957') as contract_address, 'Uniswap v1 MKR Pool' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x364508A5cA0538d8119D3BF40A284635686C98c4') as contract_address, 'dydx Perpetual' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x6B175474E89094C44Da98b954EedeAC495271d0F') as contract_address, 'DAI Token' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xD5D2b9e9bcd172D5fC8521AFd2C98Dd239F5b607') as contract_address, 'Unknown' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x241e82C79452F51fbfc89Fac6d912e021dB1a3B7') as contract_address, 'DDEX' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x12D66f87A04A9E220743712cE6d9bB1B5616B8Fc') as contract_address, 'Tornado Cash' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x5d22045DAcEAB03B158031eCB7D9d06Fad24609b') as contract_address, 'Deversifi' as product_name,  'protocol' as product_type, '2020-06-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x498b3BfaBE9F73db90D252bCD4Fa9548Cd0Fd981') as contract_address, 'Instadapp Registry' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x448a5065aeBB8E423F0896E6c5D525C040f59af3') as contract_address, 'Maker SCD' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xe80d347DF1209a76DD9d2319d62912ba98C54DDD') as contract_address, 'RenVM' as product_name,  'protocol' as product_type, '2020-06-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xB27F1DB0a7e473304A5a06E54bdf035F671400C0') as contract_address, '0x v3' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x1E0447b19BB6EcFdAe1e4AE1694b0C3659614e4e') as contract_address, 'dydx Margin' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B') as contract_address, 'Compound v2' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x34CfAC646f301356fAa8B21e94227e3583Fe3F5F') as contract_address, 'Gnosis Safe' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xc0a47dFe034B400B47bDaD5FecDa2621de6c4d95') as contract_address, 'Uniswap v1' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B') as contract_address, 'MakerDAO MCD' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x72338b82800400F5488eCa2B5A37270ba3B7A111') as contract_address, 'Paraswap - OLD' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xc1D2819CE78f3E15Ee69c6738eB1B400A26e632A') as contract_address, 'Aave v1' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x10eC0D497824e342bCB0EDcE00959142aAa766dD') as contract_address, 'Idle Finance - OLD' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x3dfd23A6c5E8BbcFc9581d2E864a68feb6a076d3') as contract_address, 'Aave Lending Core' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x45F783CCE6B7FF23B2ab2D70e416cdb7D6055f51') as contract_address, 'Curve fi - OLD' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x11111254369792b2Ca5d084aB5eEA397cA8fa48B') as contract_address, '1Inch (DEX & Liquidity Pools)' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xF92C1ad75005E6436B4EE84e88cB23Ed8A290988') as contract_address, 'Paraswap OLD' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xb529964F86fbf99a6aA67f72a27e59fA3fa4FEaC') as contract_address, 'Opyn' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x9D25057e62939D3408406975aD75Ffe834DA4cDd') as contract_address, 'Yearn Finance (all vaults)' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x77208a6000691E440026bEd1b178EF4661D37426') as contract_address, 'Totle' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x12f208476F64De6e6f933E55069Ba9596D818e08') as contract_address, 'Flexa Staking' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x78751B12Da02728F467A44eAc40F5cbc16Bd7934') as contract_address, 'Idle v3' as product_name,  'protocol' as product_type, '2020-06-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x7fC77b5c7614E1533320Ea6DDc2Eb61fa00A9714') as contract_address, 'Curve BTC Pools' as product_name,  'protocol' as product_type, '2020-06-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27') as contract_address, 'Curve All Pools (incl staking)' as product_name,  'protocol' as product_type, '2020-06-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x5B67871C3a857dE81A1ca0f9F7945e5670D986Dc') as contract_address, 'Set Protocol' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f') as contract_address, 'Uniswap v2' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x9424B1412450D0f8Fc2255FAf6046b98213B76Bd') as contract_address, 'Balancer v1' as product_name,  'protocol' as product_type, '2020-06-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xD36132E0c1141B26E62733e018f12Eb38A7b7678') as contract_address, 'Ampleforth Tokengeyser' as product_name,  'protocol' as product_type, '2020-06-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x86969d29F5fd327E1009bA66072BE22DB6017cC6') as contract_address, 'Paraswap v1' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x5f9AE054C7F0489888B1ea46824b4B9618f8A711') as contract_address, 'Melon v1' as product_name,  'protocol' as product_type, '2020-06-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x1fd169A4f5c59ACf79d0Fd5d91D1201EF1Bce9f1') as contract_address, 'MolochDAO' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xAFcE80b19A8cE13DEc0739a1aaB7A028d6845Eb3') as contract_address, 'mStable' as product_name,  'protocol' as product_type, '2020-06-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F') as contract_address, 'Synthetix' as product_name,  'protocol' as product_type, '2020-06-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x2a0c0DBEcC7E4D658f48E01e3fA353F44050c208') as contract_address, 'IDEX v1' as product_name,  'protocol' as product_type, '2020-01-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x9AAb3f75489902f3a48495025729a0AF77d4b11e') as contract_address, 'Kyber (Katalyst)' as product_name,  'protocol' as product_type, '2020-07-07' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x1F573D6Fb3F13d689FF844B4cE37794d79a7FF1C') as contract_address, 'Bancor v2' as product_name,  'protocol' as product_type, '2020-07-23' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x3e532e6222afe9Bcf02DCB87216802c75D5113aE') as contract_address, 'UMA' as product_name,  'protocol' as product_type, '2020-07-30' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x02285AcaafEB533e03A7306C55EC031297df9224') as contract_address, 'dForce Yield Market' as product_name,  'protocol' as product_type, '2020-08-13' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0e2298E3B3390e3b945a5456fBf59eCc3f55DA16') as contract_address, 'Yam Finance v1' as product_name,  'protocol' as product_type, '2020-08-13' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x3fE7940616e5Bc47b0775a0dccf6237893353bB4') as contract_address, 'Idle v4' as product_name,  'protocol' as product_type, '2020-08-14' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x71CD6666064C3A1354a3B4dca5fA1E2D3ee7D303') as contract_address, 'Mooniswap' as product_name,  'protocol' as product_type, '2020-08-25' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xe20A5C79b39bC8C363f0f49ADcFa82C2a01ab64a') as contract_address, 'tBTC Contracts v1' as product_name,  'protocol' as product_type, '2020-09-28' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xe9778E69a961e64d3cdBB34CF6778281d34667c2') as contract_address, 'NuCypher Worklock' as product_name,  'protocol' as product_type, '2020-09-29' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x4C39b37f5F20a0695BFDC59cf10bd85a6c4B7c30') as contract_address, 'Akropolis Delphi' as product_name,  'protocol' as product_type, '2020-10-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x3A97247DF274a17C59A3bd12735ea3FcDFb49950') as contract_address, 'DODO Exchange' as product_name,  'protocol' as product_type, '2020-10-19' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x26aaD4D82f6c9FA6E34D8c1067429C986A055872') as contract_address, 'CoFix' as product_name,  'protocol' as product_type, '2020-10-26' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xCB876f60399897db24058b2d58D0B9f713175eeF') as contract_address, 'Pool Together v3' as product_name,  'protocol' as product_type, '2020-10-28' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xa4c8d221d8BB851f83aadd0223a8900A6921A349') as contract_address, 'Set Protocol v2' as product_name,  'protocol' as product_type, '2020-10-28' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xB94199866Fe06B535d019C11247D3f921460b91A') as contract_address, 'Yield Protocol' as product_name,  'protocol' as product_type, '2020-10-28' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x00000000219ab540356cBB839Cbe05303d7705Fa') as contract_address, 'Eth 2.0 (deposit contract)' as product_name,  'protocol' as product_type, '2020-11-06' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x878F15ffC8b894A1BA7647c7176E4C01f74e140b') as contract_address, 'Hegic' as product_name,  'protocol' as product_type, '2020-11-09' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xfA5047c9c78B8877af97BDcb85Db743fD7313d4a') as contract_address, 'Keeper DAO' as product_name,  'protocol' as product_type, '2020-11-19' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x3d5BC3c8d13dcB8bF317092d84783c2697AE9258') as contract_address, 'CREAM v1' as product_name,  'protocol' as product_type, '2020-11-24' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x7a9701453249e84fd0D5AfE5951e9cBe9ed2E90f') as contract_address, 'TrueFi' as product_name,  'protocol' as product_type, '2020-11-25' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x67B66C99D3Eb37Fa76Aa3Ed1ff33E8e39F0b9c7A') as contract_address, 'Alpha Homora v1' as product_name,  'protocol' as product_type, '2020-11-26' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9') as contract_address, 'Aave v2' as product_name,  'protocol' as product_type, '2020-12-03' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xc2EdaD668740f1aA35E4D8f227fB8E17dcA888Cd') as contract_address, 'SushiSwap v1' as product_name,  'protocol' as product_type, '2020-12-09' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xedfC81Bf63527337cD2193925f9C0cF2D537AccA') as contract_address, 'Cover Protocol v1' as product_name,  'protocol' as product_type, '2020-12-11' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xA51156F3F1e39d1036Ca4ba4974107A1C1815d1e') as contract_address, 'Perpetual Protocol' as product_name,  'protocol' as product_type, '2020-12-17' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x6354E79F21B56C11f48bcD7c451BE456D7102A36') as contract_address, 'BadgerDAO' as product_name,  'protocol' as product_type, '2020-12-29' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x9abd0b8868546105F6F48298eaDC1D9c82f7f683') as contract_address, 'Notional Finance v1' as product_name,  'protocol' as product_type, '2021-01-18' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xE75D77B1865Ae93c7eaa3040B038D7aA7BC02F70') as contract_address, 'Origin Dollar' as product_name,  'protocol' as product_type, '2021-01-22' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x7C06792Af1632E77cb27a558Dc0885338F4Bdf8E') as contract_address, 'Opyn v2' as product_name,  'protocol' as product_type, '2021-02-25' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xCC88a9d330da1133Df3A7bD823B95e52511A6962') as contract_address, 'Reflexer' as product_name,  'protocol' as product_type, '2021-03-04' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xa4F1671d3Aee73C05b552d57f2d16d3cfcBd0217') as contract_address, 'Vesper' as product_name,  'protocol' as product_type, '2021-03-10' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x5D9972dD3Ba5602574ABeA6bF9E1713568D49903') as contract_address, 'Benchmark Protocol' as product_name,  'protocol' as product_type, '2021-03-22' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xB17640796e4c27a39AF51887aff3F8DC0daF9567') as contract_address, 'Stake DAO' as product_name,  'protocol' as product_type, '2021-03-29' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xA39739EF8b0231DbFA0DcdA07d7e29faAbCf4bb2') as contract_address, 'Liquity' as product_name,  'protocol' as product_type, '2021-04-05' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x284D7200a0Dabb05ee6De698da10d00df164f61d') as contract_address, 'Harvest Finance' as product_name,  'protocol' as product_type, '2021-04-16' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x1F98431c8aD98523631AE4a59f267346ea31F984') as contract_address, 'Uniswap v3' as product_name,  'protocol' as product_type, '2021-05-05' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x4B8d90D68F26DEF303Dcb6CFc9b63A1aAEC15840') as contract_address, 'Barnbridge Smart Yield v1' as product_name,  'protocol' as product_type, '2021-06-14' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xF403C135812408BFbE8713b5A23a04b3D48AAE31') as contract_address, 'Convex Finance v1' as product_name,  'protocol' as product_type, '2021-06-14' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xc21D353FF4ee73C572425697f4F5aaD2109fe35b') as contract_address, 'Alchemix v1' as product_name,  'protocol' as product_type, '2021-06-14' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x99c666810bA4Bf9a4C2318CE60Cb2c279Ee2cF56') as contract_address, 'Homora v2' as product_name,  'protocol' as product_type, '2021-06-23' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xBA12222222228d8Ba445958a75a0704d566BF2C8') as contract_address, 'Balancer v2' as product_name,  'protocol' as product_type, '2021-08-04' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xA625AB01B08ce023B2a342Dbb12a16f2C8489A8F') as contract_address, 'Alpaca Finance' as product_name,  'protocol' as product_type, '2021-08-13' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x08FB62c84909dA3Aa5F59E01763E5FDC62De76e9') as contract_address, 'Gamma Strategies' as product_name,  'protocol' as product_type, '2021-08-20' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x8481a6EbAf5c7DABc3F7e09e44A89531fd31F822') as contract_address, 'Goldfinch' as product_name,  'protocol' as product_type, '2021-09-24' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xc57D000000000000000000000000000000000001') as contract_address, 'Celsius' as product_name,  'custodian' as product_type, '2020-12-02' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xC57D000000000000000000000000000000000002') as contract_address, 'BlockFi' as product_name,  'custodian' as product_type, '2020-12-02' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xC57d000000000000000000000000000000000003') as contract_address, 'Nexo' as product_name,  'custodian' as product_type, '2020-12-02' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xc57d000000000000000000000000000000000004') as contract_address, 'inLock' as product_name,  'custodian' as product_type, '2020-12-02' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xC57D000000000000000000000000000000000005') as contract_address, 'Ledn' as product_name,  'custodian' as product_type, '2020-12-02' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xC57d000000000000000000000000000000000006') as contract_address, 'Hodlnaut' as product_name,  'custodian' as product_type, '2020-12-02' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xC57d000000000000000000000000000000000007') as contract_address, 'Binance' as product_name,  'custodian' as product_type, '2021-01-12' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xc57D000000000000000000000000000000000008') as contract_address, 'Coinbase' as product_name,  'custodian' as product_type, '2021-01-12' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xc57d000000000000000000000000000000000009') as contract_address, 'Kraken' as product_name,  'custodian' as product_type, '2021-01-12' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xc57d000000000000000000000000000000000010') as contract_address, 'Gemini' as product_name,  'custodian' as product_type, '2021-01-12' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xC57d000000000000000000000000000000000011') as contract_address, 'FTX' as product_name,  'custodian' as product_type, '2021-05-04' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xC57d000000000000000000000000000000000012') as contract_address, 'Crypto.com' as product_name,  'custodian' as product_type, '2021-07-06' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xc57d000000000000000000000000000000000013') as contract_address, 'Yield.app' as product_name,  'custodian' as product_type, '2021-07-06' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xefa94DE7a4656D787667C749f7E1223D71E9FD88') as contract_address, 'Pangolin' as product_name,  'protocol' as product_type, '2021-10-12' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0CED6166873038Ac0cc688e7E6d19E2cBE251Bf0') as contract_address, 'Centrifuge Tinlake' as product_name,  'protocol' as product_type, '2021-10-12' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x835482FE0532f169024d5E9410199369aAD5C77E') as contract_address, 'Rari Capital' as product_name,  'protocol' as product_type, '2021-10-12' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xd96f48665a1410C0cd669A88898ecA36B9Fc2cce') as contract_address, 'Abracadabra' as product_name,  'protocol' as product_type, '2021-11-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x48D49466CB2EFbF05FaA5fa5E69f2984eDC8d1D7') as contract_address, 'Premia Finance' as product_name,  'protocol' as product_type, '2021-11-04' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000001') as contract_address, 'Anchor' as product_name,  'protocol' as product_type, '2021-04-26' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000002') as contract_address, 'Bunny' as product_name,  'protocol' as product_type, '2021-04-26' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000003') as contract_address, 'Venus' as product_name,  'protocol' as product_type, '2021-04-26' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000004') as contract_address, 'Thorchain' as product_name,  'protocol' as product_type, '2021-04-26' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000005') as contract_address, 'Pancakeswap v1' as product_name,  'protocol' as product_type, '2021-04-26' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000006') as contract_address, 'Yearn yvDAI v2' as product_name,  'token' as product_type, '2021-05-25' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000007') as contract_address, 'Yearn yvUSDC v2' as product_name,  'token' as product_type, '2021-05-25' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000008') as contract_address, 'Yearn ycrvstETH v2' as product_name,  'token' as product_type, '2021-05-25' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000009') as contract_address, 'Curve 3pool LP (3Crv)' as product_name,  'token' as product_type, '2021-05-25' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000010') as contract_address, 'Curve sETH LP (eCrv)' as product_name,  'token' as product_type, '2021-05-25' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000011') as contract_address, 'Idle DAI v4 (idleDAIYield)' as product_name,  'token' as product_type, '2021-07-05' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000012') as contract_address, 'Idle USDT v4 (idleUSDTYield)' as product_name,  'token' as product_type, '2021-07-05' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000013') as contract_address, 'Convex stethCrv (cvxstethCrv)' as product_name,  'token' as product_type, '2021-10-28' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000014') as contract_address, 'Convex 3CRV (cvx3CRV)' as product_name,  'token' as product_type, '2021-10-28' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000015') as contract_address, 'Convex mimCrv (cvxmimCrv)' as product_name,  'token' as product_type, '2021-10-28' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xaE7b92C8B14E7bdB523408aE0A6fFbf3f589adD9') as contract_address, 'Popsicle Finance' as product_name,  'protocol' as product_type, '2021-11-10' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x1344A36A1B56144C3Bc62E7757377D288fDE0369') as contract_address, 'Notional Finance v2' as product_name,  'protocol' as product_type, '2021-11-22' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x575409F8d77c12B05feD8B455815f0e54797381c') as contract_address, 'OlympusDAO' as product_name,  'protocol' as product_type, '2021-11-29' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x25751853Eab4D0eB3652B5eB6ecB102A2789644B') as contract_address, 'Ribbon Finance v2' as product_name,  'protocol' as product_type, '2021-12-06' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xd89a09084555a7D0ABe7B111b1f78DFEdDd638Be') as contract_address, 'Pool Together v4' as product_name,  'protocol' as product_type, '2021-12-09' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x60aE616a2155Ee3d9A68541Ba4544862310933d4') as contract_address, 'Trader Joe' as product_name,  'protocol' as product_type, '2021-12-14' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000016') as contract_address, 'Origin OUSD' as product_name,  'token' as product_type, '2022-01-24' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x2BB8de958134AFd7543d4063CaFAD0b7c6de08BC') as contract_address, 'Ondo' as product_name,  'protocol' as product_type, '2022-01-28' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x7e6d3b1161DF9c9c7527F68d651B297d2Fdb820B') as contract_address, 'Enzyme v3' as product_name,  'protocol' as product_type, '2022-02-16' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x453D4Ba9a2D594314DF88564248497F7D74d6b2C') as contract_address, 'Beefy' as product_name,  'protocol' as product_type, '2022-03-04' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0xfdA462548Ce04282f4B6D6619823a7C64Fdc0185') as contract_address, 'Angle' as product_name,  'protocol' as product_type, '2022-03-08' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x66357dCaCe80431aee0A7507e2E361B7e2402370') as contract_address, 'Platypus' as product_name,  'protocol' as product_type, '2022-03-18' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000017') as contract_address, 'FODL' as product_name,  'protocol' as product_type, '2022-03-24' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x5C6374a2ac4EBC38DeA0Fc1F8716e5Ea1AdD94dd') as contract_address, 'Alchemix v2' as product_name,  'protocol' as product_type, '2022-03-24' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000018') as contract_address, 'Bundle: Gelt + mStable + Aave v2' as product_name,  'protocol' as product_type, '2022-04-14' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000019') as contract_address, 'Yeti Finance' as product_name,  'protocol' as product_type, '2022-04-28' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000020') as contract_address, 'Babylon Finance' as product_name,  'protocol' as product_type, '2022-05-12' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000021') as contract_address, 'Vector' as product_name,  'protocol' as product_type, '2022-05-12' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000022') as contract_address, 'Bancor v3' as product_name,  'protocol' as product_type, '2022-05-12' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000023') as contract_address, 'Ease' as product_name,  'protocol' as product_type, '2022-05-31' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000024') as contract_address, 'Iron Bank' as product_name,  'protocol' as product_type, '2022-06-08' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000025') as contract_address, 'Stakewise operated (3 ETH / validator)' as product_name,  'eth2slashing' as product_type, '2022-06-08' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000026') as contract_address, 'Stakewise 3rd party (3 ETH / validator)' as product_name,  'eth2slashing' as product_type, '2022-06-08' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000027') as contract_address, 'Nested' as product_name,  'protocol' as product_type, '2022-07-05' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000028') as contract_address, 'Euler' as product_name,  'protocol' as product_type, '2022-08-29' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x3D6bA331e3D9702C5e8A8d254e5d8a285F223aba') as contract_address, 'GMX' as product_name,  'protocol' as product_type, '2022-09-01' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000029') as contract_address, 'Sherlock' as product_name,  'sherlock' as product_type, '2022-10-17' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000030') as contract_address, 'Gearbox V2' as product_name,  'protocol' as product_type, '2022-10-31' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000031') as contract_address, 'Aura' as product_name,  'protocol' as product_type, '2022-11-16' as date_added, 'v1' as syndicate UNION ALL
SELECT lower('0x0000000000000000000000000000000000000032') as contract_address, 'Enzyme v4' as product_name,  'protocol' as product_type, '2022-11-16' as date_added, 'v1' as syndicate