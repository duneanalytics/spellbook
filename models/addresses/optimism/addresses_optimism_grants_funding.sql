{{config(
  tags = ['dunesql','static'],
  alias = alias('grants_funding'),
	post_hook='{{ expose_spells(\'["optimism"]\',
		"sector",
		"addresses",
		\'["msilb7"]\') }}')}}

WITH entries AS (
SELECT
	category
	, address
	, proposal_name
	, funding_source

FROM (VALUES

 --Foundation Distributor Wallets
 ('OP Foundation',	0x2501c477d0a35545a387aa4a3eee4292a9a8b3f0,	'OP Foundation Wallet',		'OP Foundation Grants')
,('OP Foundation',	0x19793c7824be70ec58bb673ca42d2779d12581be,	'OP Foundation Sentry Wallet',	'OP Foundation Grants')
--Airdrop Distributor Wallets
,('OP Foundation',	0xbe9a9b1b07f027130e56d8569d1aea5dd5a86013,	'Airdrop #2 Distributor',	'OP Foundation Airdrops')
,('OP Foundation',	0xfedfaf1a10335448b7fa0268f56d2b44dbd357de,	'Airdrop #1 Distributor',	'OP Foundation Airdrops')
--Project Distribution to Wallets
,('Project',	0x0d162447b8df47c2e7910441bf3c8c1b55b9b124,	'Uniswap V3',	'Phase 0')
,('Project',	0x1bd315e3f029083da8731a6b1c6f2959a0d15376,	'1inch',	'Phase 0')
,('Project',	0xf5e9d550c3c50364d630edb4753be404cd109121,	'Celer',	'Phase 0')
,('Project',	0xcd150d9cfdb39d287d69b9b1bb7111e58414aca8,	'Aave',	'Phase 0')
,('Project',	0x7ee753e87f20eb3262967c2fc54762b0b4ff4d75,	'Chainlink',	'Phase 0')
,('Project',	0x59672d112d680ce34c20ff1507197993cc0ba430,	'Polynomial Protocol',	'Phase 0')
,('Project',	0xba9493f97bef07a272998139a268398b80ab9dd5,	'Zeroex (0x)',	'Phase 0')
,('Project',	0x5b8f3fb479571eca6a06240b21926db586cdf10f,	'Aelin',	'Phase 0')
,('Project',	0x9d439e524f214fb0cb5fa42030e578f60e64d98c,	'Slingshot',	'Phase 0')
,('Project',	0x75f7b331bbdbacafe1ec5ba5215a0fbfc002b3b8,	'WePiggy',	'Phase 0')
,('Project',	0x489863b61c625a15c74fb4c21486bacb4a3937ab,	'Thales',	'Phase 0')
,('Project',	0xa9cd2563c2cce7db8edbe7aacdb0393c68151a28,	'Pika Protocol',	'Phase 0')
,('Project',	0xaf5a0068f5465260a1a88a6264d0dce4469609cf,	'Layer2dao',	'Phase 0')
,('Project',	0x9644a6920bd0a1923c2c6c1dddf691b7a42e8a65,	'Synthetix',	'Phase 0')
,('Project',	0x1dd174bf2e090d0f1b1360ac8905780429b2642b,	'Clipper',	'Phase 0')
,('Project',	0x8602ee2f8aaeb671e409b26d48e36dd8cc3b7ed7,	'Zipswap',	'Phase 0')
,('Project',	0x82d2242257115351899894ef384f779b5ba8c695,	'Kwenta',	'Phase 0')
,('Project',	0xc988107688b750dee6237b85a3ca49ba0a65dab0,	'Hop Protocol',	'Phase 0')
,('Project',	0x2431cbdc0792f5485c4cb0a9bef06c4f21541d52,	'Synapse',	'Phase 0')
,('Project',	0x3576aafaffe4c07f894447265b391856377157db,	'Connext',	'Phase 0')
,('Project',	0x3204ac6f848e05557c6c7876e09059882e07962f,	'Rubicon',	'Phase 0')
,('Project',	0x01ea687be2937d4bfd9e302b8dbd3be8d9bdb14e,	'Gelato',	'Phase 0')
,('Project',	0x392ac17a9028515a3bfa6cce51f8b70306c6bd43,	'Stargate Finance',	'Phase 0')
,('Project',	0xb6dacae4ef97b4817d54df8e005269f509f803f9,	'Lyra',	'Phase 0')
,('Project',	0xdcf664d0f76e99eaa2dbd569474d0e75dc899fcd,	'Perpetual Protocol',	'Phase 0')
,('Project',	0x69c1a51711b061e5935c648beb16e349898b17df,	'dForce',	'Season 1')
,('Project',	0x308810881807189cae91950888b2cb73a1cc5920,	'Mean Finance',	'Season 1')
,('Project',	0x2a185c8a3c63d7bfe63ad5d950244ffe9d0a4b60,	'Beethoven X',	'Season 1')
,('Project',	0xc146a672c85b408a9a7bc00f74ad567b7684134c,	'WardenSwap',	'Season 1')
,('Project',	0x7a79e2e867d36a91bb47e0929787305c95e793c5,	'Pickle Finance',	'Season 1')
,('Project',	0x0740374d74Ee4424398f3A16fcC5b301FCf47F92,	'Paraswap',	'Season 1')
,('Project',	0x9531c059098e3d194ff87febb587ab07b30b1306,	'Rotki',	'Season 1')
,('Project',	0xa60b3402051ec75b17abb81b4c92bf88d5e49aa8,	'Candide Wallet',	'Season 1')
,('Project',	0x719cf569ffc5c09625b2b5c68a1078f73c6a1a6f,	'Superfluid',	'Season 1')
,('Project',	0x641f26c67a5d0829ae61019131093b6a7c7d18a3,	'Hundred Finance',	'Season 1')
,('Project',	0x31b73b0549e0151b6233c5d179580b8a840b83fb,	'Biconomy',	'Season 1')
,('Project',	0x2319D8e71268FF128e3a9CfA0E62d32596d4DC02,	'Rocket Pool',	'Season 1')
,('Project',	0xf6a4475a006b28b9996c92bee27deedc38032cde,	'Boardroom',	'Season 1')
,('Project',	0x38138586aedb29b436eab16105b09c317f5a79dd,	'xToken Terminal / Gamma Strategies',	'Season 1')
,('Project',	0x4aba01fb8e1f6bfe80c56deb367f19f35df0f4ae,	'Beefy Finance',	'Season 1')
,('Project',	0xb1a8d1d6dc07ca0e1e78a0004aa0bb034fa73d60,	'QiDao',	'Season 1')

,('Project',	0x352Fb838A3ae9b0ef2f0EBF24191AcAf4aB9EcEc,	'dHedge',	'Season 2')
,('Project',	0x787B66d5281D45126bf0e0f3C9aC682D1E119d6C,	'Across Protocol',	'Season 2')
,('Project',	0xbFBceA2a72Bc2e62E967fA0c79458dD507708469,	'Bankless Academy',	'Season 2')
,('Project',	0x8cadb20a4811f363dadb863a190708bed26245f8,	'Revert Finance',	'Season 2')
,('Project',	0x05d235d8Ba95bfc457f9a11F64cf869f0f3f60F9,	'Kromatika',	'Season 2')
,('Project',	0x10850762bac0dc6660630c1effe188a7cbfddc88,	'OptiChads',	'Season 2')
,('Project',	0xa6e8772af29b29B9202a073f8E36f447689BEef6,	'Interest Protocol',	'Season 2')
,('Project',	0x489863b61C625a15C74FB4C21486baCb4A3937AB,	'Thales',	'Season 2') --This is Overtime Markets, but the address is the same as Thales S0, so we'll label as Thales
,('Project',	0x1219Bfa3A499548507b4917E33F17439b67A2177,	'SushiSwap',	'Season 2')
,('Project',	0x63D5554b748598C191aB0cBd0E0e1C638FFEBEe6,	'Tarot',	'Season 2')
,('Project',	0x2696170bf5fD36320c3932fd9e85fe5b798385FE,	'Otterspace',	'Season 2')
,('Project',	0x9d62e3f4f5a2ef4F446dA692B07860F3C78CEAa4,	'Rainbow Wallet',	'Season 2')
,('Project',	0x627f84bb4bBA3333f253F09fe22A445F195bCF34,	'Karma 1',	'Season 2') --two karmas
,('Project',	0x627f84bb4bBA3333f253F09fe22A445F195bCF34,	'Karma 2',	'Season 2')
,('Project',	0x4D218d77cDdD700565826A31837277e9e2Ea0564,	'LiFi',	'Season 2')
,('Project',	0xF5d9D6133b698cE29567a90Ab35CfB874204B3A7,	'Yearn',	'Season 2')
,('Project',	0x90103beDCfbE1eeE44ded89cEd88bA8503580b3D,	'Dope Wars',	'Season 2')
,('Project',	0x3EDf6868d7c42863E44072DaEcC16eCA2804Dea1,	'Safe',	'Season 2')
,('Project',	0xC224bf25Dcc99236F00843c7D8C4194abE8AA94a,	'Alchemix',	'Season 2')
,('Project',	0x3245d3204eeb67afba7b0ba9143e8081365e08a6,	'Angle',	'Season 2')
,('Project',	0x29C8942C7fDDe7fCFB46eb6d0FE62f9dED75361c,	'InsureDAO',	'Season 2')
,('Project',	0xD166EEdf272B860E991d331B71041799379185D5,	'Curve',	'Season 2')
,('Project',	0xfB0dADb835fAdE151aBf6780BeAfB12FC5BA0e1F,	'PoolTogether',	'Season 2') --dupe
,('Project',	0xe497285e466227F4E8648209E34B465dAA1F90a0,	'Overnight',	'Season 2')
,('Project',	0x803CE4f6A0e064a01C56f7Ca21223a7b65e4b869,	'Socket',	'Season 2')
,('Project',	0x2431BFA47bB3d494Bd720FaC71960F27a54b6FE7,	'EthernautDAO',	'Season 2')
,('Project',	0x99b36fDbC582D113aF36A21EBa06BFEAb7b9bE12,	'Tally Ho',	'Season 2')
,('Project',	0x08a3c2A819E3de7ACa384c798269B3Ce1CD0e437,	'DefiLlama',	'Season 2')
,('Project',	0x0e2cd794602152772a19c5eb7bf221ccafdabd9d,	'Agora',	'Season 2')
,('Project',	0x41693B7c0E41CBEa0d4F68Dc73898CB79e390f29,	'Mochi',	'Season 2')
,('Project',	0xb074ec6c37659525EEf2Fb44478077901F878012,	'Velodrome',	'Season 2') --dupe

,('Project',	0xd873d124d79ca220ebed2b93eb1205c61d228864,	'Atomic Links',	'Season 3')
,('Project',	0x5eff4b8c042aa6003cbda6e32c4aa23657d3b6af,	'Attestation Station Interface',	'Season 3')
,('Project',	0xef43993d36145a74ed34bc17656789a28a7c60c7,	'Clique',	'Season 3')
,('Project',	0x92978b0d605c5ec59f9dbd9f11b689604f64ca3a,	'DeFunds',	'Season 3')
,('Project',	0xf473edfaaef8cb2e5cdd246b7d5b285e4f56f721,	'Edge Wallet',	'Season 3')
,('Project',	0x1d7b8d3c6c896d7eb4f9118ea377c359073d4083,	'ENS Wildcard Domains',	'Season 3')
,('Project',	0xb476ee7d610dae7b23b671ebc7bd6112e9772969,	'Front running protection',	'Season 3')
,('Project',	0x627f84bb4bba3333f253f09fe22a445f195bcf34,	'Karma delegate registry',	'Season 3')
,('Project',	0x1a44ae850933cc7b3bb464da6d500df1745df0ba,	'Metronomo',	'Season 3')
,('Project',	0x9531c059098e3d194ff87febb587ab07b30b1306,	'Rotki',	'Season 3')
,('Project',	0x61ee892250c03c1d7bc673e661be87116caba2ac,	'Crypto LDFM',	'Season 3')
,('Project',	0x5891be896ed4a79ed928c55b17fbbecdb46f8a00,	'KyberSwap',	'Season 3')
,('Project',	0x7c8126ef43c09c22bf0ccdf7426180e6c48068a5,	'Mux',	'Season 3')
,('Project',	0x07963001638c2e8283ef7f3dabf4242eaf5d4f79,	'Nested',	'Season 3')
,('Project',	0x66a44dab44d21e2631e61b21759a30b73e94ae69,	'OPWeave',	'Season 3')
,('Project',	0xf95ced2c9a8defd9378208ad84019866a3787233,	'Parcel',	'Season 3')
,('Project',	0xfc5538e1e9814ed6487b407fad7b5710739a1cc2,	'Premia',	'Season 3')
,('Project',	0x3204ac6f848e05557c6c7876e09059882e07962f,	'Rubicon',	'Season 3')
,('Project',	0x784b82a27029c9e114b521abcc39d02b3d1deaf2,	'Sonne Finance',	'Season 3')
,('Project',	0x489863b61c625a15c74fb4c21486bacb4a3937ab,	'Thales',	'Season 3')
,('Project',	0x6b492bbbe311f3c1e15e3d4ccc00cc2a412089ff,	'The Optimistic Series',	'Season 3')
,('Project',	0x1a27cdf069f2e8cc5b77b72bea43851086af5d83,	'Via Protocol',	'Season 3')

,('Project',	0x276dcbb22b41F272F626C6D7348e93618C0ec5B4,	'Airgap Wallet',	'Season 3')
,('Project',	0xb053216998E4E11c655e3C9CEe086Bf3cc7c8Ed7,	'Chaintrap',	'Season 3')
,('Project',	0xD3581888a0890Fb3270F8349588F3f4Cc25Fb2c9,	'CharmVerse',	'Season 3')
,('Project',	0x7601630eC802952ba1ED2B6e4db16F699A0a5A87,	'DeNotify',	'Season 3')

,('Project',	0x11Ee133A1408FE2d7c62296D7eB33F234b774503,	'dm3 Protocol',	'Season 3')
,('Project',	0x1f0A4eb02d9BEfBd1538E8D230699d4e434CDbEE,	'Footprint Analytics',	'Season 3')
,('Project',	0x17C2D875CB397D813eAE817DaFD25807E348Df07,	'Herodotus',	'Season 3')
,('Project',	0x37c752aA952CFb62cc403fd810F2a01801675Bcc,	'Infinity Keys',	'Season 3')
,('Project',	0x7878AefB660AF2B0928b6E1A8AcD2b97ECB37C2c,	'Inverter',	'Season 3')
,('Project',	0xdd347ad6aA6D26ae6840ed3574E6Ce4b0E002066,	'JiffyScan',	'Season 3')
,('Project',	0x8b6c27ec466923fad66Ada94c78AA320eA876969,	'Opti.domains',	'Season 3')
,('Project',	0x7b57c388e6149b5c197B925037602d5B6bafFbCc,	'Synpress',	'Season 3')
,('Project',	0x93501fddb36ea67ffa2c361b3da035c142299d4f,	'Another World',	'Season 3')
,('Project',	0x99a9cC2Aaf54F6bE94A437641692A72810f6E9d5,	'Bored Town',	'Season 3')
,('Project',	0x69C1a51711B061E5935c648beb16e349898B17dF,	'dForce',	'Season 3')
,('Project',	0x926E2d35053e2d9d1CC72C6eDA23F13a33270a76,	'Fairmint',	'Season 3')
,('Project',	0xDE798cD9C53F4806B9Cc7dD27aDf7c641540167c,	'Geo Web',	'Season 3')
,('Project',	0x4D9339dd97db55e3B9bCBE65dE39fF9c04d1C2cd,	'Giveth',	'Season 3')
,('Project',	0xB0E9021096d7d6458D747CFe959E5E066Fc52dED,	'Metalswap',	'Season 3')
,('Project',	0x5aB0Fdc6F0CA9Ea178c95cECCf944A4ce1AedDdD,	'Galxe',	'Season 3')
,('Project',	0x14fF422F5bacc52E20BE81B184F4CE1f0367402B,	'Optiland Quests',	'Season 3')
,('Project',	0x5B5ECfc8122bA166b21d6Ea26268Ef97e09B2E9F,	'Paytrie',	'Season 3')
,('Project',	0xbD72a3Cd66B3e40E5151B153164905FD65b55145,	'Rabbit Hole',	'Season 3')
,('Project',	0x6268d07327f4fb7380732dc6d63d95F88c0E083b,	'ShapeShift',	'Season 3')
,('Project',	0x7fB50BBabeDEE52b8760Ba15c0c199aF33Fc2EfA,	'Threshold',	'Season 3')
,('Project',	0x76d266dfd3754f090488ae12f6bd115cd7e77ebd,	'Vesper',	'Season 3')
-- Announced Partner Fund
,('Project',	0xfb0dadb835fade151abf6780beafb12fc5ba0e1f,	'PoolTogether',	'Partner Fund')
,('Project',	0x30688fcd63d0f2d067a7fe264b3367c348e9b031,	'Velodrome',	'Partner Fund')
,('Project',	0x13838884271ec954cb036d0b29d404afab5eae2a,	'CryptoTesters',	'Partner Fund')
,('Project',	0xeb952357394b6668684a4890bd49d8a4c3e19ec0,	'Okex',	'Partner Fund')
,('Project',	0xb1ea5a3e5ea7fa1834d48058ecda26d8c59e8251,	'Quix',	'Partner Fund')
,('Project',	0xf42279467d821bcdf40b50e9a5d2caccc4cf5b30,	'Quests on Coinbase Wallet',	'Partner Fund') --Quest 1 - DEX
,('Project',	0x9f4f2b8bda8d2d3832021b3119747470ea86a183,	'Quests on Coinbase Wallet',	'Partner Fund') --Quest 2 - Delegation
,('Project',	0x1fe95e0497a0E38AFBE18Bd19B9a2b42116880f0,	'Quests on Coinbase Wallet',	'Partner Fund') --Quest 3 - Attestation
,('Project',	0x12d9aEF514EE8Bc3f7B2d523ae26164632b71acB,	'Quests on Coinbase Wallet',	'Partner Fund') --Quest 4 - Attestation
,('Project',	0x0b1CEf4cAb511426B001f430b8d000D5a8C83AD0,	'Quests on Coinbase Wallet',	'Partner Fund') --Quest 5 - Attestation
,('Project',	0x23fd464e0b0ee21cedeb929b19cabf9bd5215019,	'Exactly',	'Partner Fund')
,('Project',	0x91cE2F083d59B832f95f90aA0997168ae051a98A,	'Lido',		'Partner Fund')
-- Distributions to Rewarder contracts are not listed

   ) AS x (category, address, proposal_name, funding_source)

)

SELECT
   category
   , entries.address
   , cast(coalesce(pnm.project_name, entries.proposal_name) as varchar) as project_name
   , cast(entries.proposal_name as varchar) as proposal_name
   , cast(entries.funding_source as varchar) as funding_source
FROM entries
LEFT JOIN {{ ref('op_token_distributions_optimism_project_name_mapping') }} pnm
        ON pnm.proposal_name = entries.proposal_name
