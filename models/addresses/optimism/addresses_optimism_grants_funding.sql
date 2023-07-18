{{config(alias = alias('grants_funding'),
    tags=['static', 'dunesql'],
	post_hook='{{ expose_spells(\'["optimism"]\',
		"sector",
		"addresses",
		\'["msilb7"]\') }}')}}

WITH entries AS (
SELECT
	category
	, address AS address
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
,('Project',	0x0740374d74ee4424398f3a16fcc5b301fcf47f92,	'Paraswap',	'Season 1')
,('Project',	0x9531c059098e3d194ff87febb587ab07b30b1306,	'Rotki',	'Season 1')
,('Project',	0xa60b3402051ec75b17abb81b4c92bf88d5e49aa8,	'Candide Wallet',	'Season 1')
,('Project',	0x719cf569ffc5c09625b2b5c68a1078f73c6a1a6f,	'Superfluid',	'Season 1')
,('Project',	0x641f26c67a5d0829ae61019131093b6a7c7d18a3,	'Hundred Finance',	'Season 1')
,('Project',	0x31b73b0549e0151b6233c5d179580b8a840b83fb,	'Biconomy',	'Season 1')
,('Project',	0x2319d8e71268ff128e3a9cfa0e62d32596d4dc02,	'Rocket Pool',	'Season 1')
,('Project',	0xf6a4475a006b28b9996c92bee27deedc38032cde,	'Boardroom',	'Season 1')
,('Project',	0x38138586aedb29b436eab16105b09c317f5a79dd,	'xToken Terminal / Gamma Strategies',	'Season 1')
,('Project',	0x4aba01fb8e1f6bfe80c56deb367f19f35df0f4ae,	'Beefy Finance',	'Season 1')
,('Project',	0xb1a8d1d6dc07ca0e1e78a0004aa0bb034fa73d60,	'QiDao',	'Season 1')

,('Project',	0x352fb838a3ae9b0ef2f0ebf24191acaf4ab9ecec,	'dHedge',	'Season 2')
,('Project',	0x787b66d5281d45126bf0e0f3c9ac682d1e119d6c,	'Across Protocol',	'Season 2')
,('Project',	0xbfbcea2a72bc2e62e967fa0c79458dd507708469,	'Bankless Academy',	'Season 2')
,('Project',	0x8cadb20a4811f363dadb863a190708bed26245f8,	'Revert Finance',	'Season 2')
,('Project',	0x05d235d8ba95bfc457f9a11f64cf869f0f3f60f9,	'Kromatika',	'Season 2')
,('Project',	0x10850762bac0dc6660630c1effe188a7cbfddc88,	'OptiChads',	'Season 2')
,('Project',	0xa6e8772af29b29b9202a073f8e36f447689beef6,	'Interest Protocol',	'Season 2')
,('Project',	0x489863b61c625a15c74fb4c21486bacb4a3937ab,	'Thales',	'Season 2') --This is Overtime Markets, but the address is the same as Thales S0, so we'll label as Thales
,('Project',	0x1219bfa3a499548507b4917e33f17439b67a2177,	'SushiSwap',	'Season 2')
,('Project',	0x63d5554b748598c191ab0cbd0e0e1c638ffebee6,	'Tarot',	'Season 2')
,('Project',	0x2696170bf5fd36320c3932fd9e85fe5b798385fe,	'Otterspace',	'Season 2')
,('Project',	0x9d62e3f4f5a2ef4f446da692b07860f3c78ceaa4,	'Rainbow Wallet',	'Season 2')
,('Project',	0x627f84bb4bba3333f253f09fe22a445f195bcf34,	'Karma 1',	'Season 2') --two karmas
,('Project',	0x627f84bb4bba3333f253f09fe22a445f195bcf34,	'Karma 2',	'Season 2')
,('Project',	0x4d218d77cddd700565826a31837277e9e2ea0564,	'LiFi',	'Season 2')
,('Project',	0xf5d9d6133b698ce29567a90ab35cfb874204b3a7,	'Yearn',	'Season 2')
,('Project',	0x90103bedcfbe1eee44ded89ced88ba8503580b3d,	'Dope Wars',	'Season 2')
,('Project',	0x3edf6868d7c42863e44072daecc16eca2804dea1,	'Safe',	'Season 2')
,('Project',	0xc224bf25dcc99236f00843c7d8c4194abe8aa94a,	'Alchemix',	'Season 2')
,('Project',	0x3245d3204eeb67afba7b0ba9143e8081365e08a6,	'Angle',	'Season 2')
,('Project',	0x29c8942c7fdde7fcfb46eb6d0fe62f9ded75361c,	'InsureDAO',	'Season 2')
,('Project',	0xd166eedf272b860e991d331b71041799379185d5,	'Curve',	'Season 2')
,('Project',	0xfb0dadb835fade151abf6780beafb12fc5ba0e1f,	'PoolTogether',	'Season 2') --dupe
,('Project',	0xe497285e466227f4e8648209e34b465daa1f90a0,	'Overnight',	'Season 2')
,('Project',	0x803ce4f6a0e064a01c56f7ca21223a7b65e4b869,	'Socket',	'Season 2')
,('Project',	0x2431bfa47bb3d494bd720fac71960f27a54b6fe7,	'EthernautDAO',	'Season 2')
,('Project',	0x99b36fdbc582d113af36a21eba06bfeab7b9be12,	'Tally Ho',	'Season 2')
,('Project',	0x08a3c2a819e3de7aca384c798269b3ce1cd0e437,	'DefiLlama',	'Season 2')
,('Project',	0x0e2cd794602152772a19c5eb7bf221ccafdabd9d,	'Agora',	'Season 2')
,('Project',	0x41693b7c0e41cbea0d4f68dc73898cb79e390f29,	'Mochi',	'Season 2')
,('Project',	0xb074ec6c37659525eef2fb44478077901f878012,	'Velodrome',	'Season 2') --dupe

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

,('Project',	0x276dcbb22b41f272f626c6d7348e93618c0ec5b4,	'Airgap Wallet',	'Season 3')
,('Project',	0xb053216998e4e11c655e3c9cee086bf3cc7c8ed7,	'Chaintrap',	'Season 3')
,('Project',	0xd3581888a0890fb3270f8349588f3f4cc25fb2c9,	'CharmVerse',	'Season 3')
,('Project',	0x7601630ec802952ba1ed2b6e4db16f699a0a5a87,	'DeNotify',	'Season 3')
,('Project',	0x11ee133a1408fe2d7c62296d7eb33f234b774503,	'dm3 Protocol',	'Season 3')
,('Project',	0x1f0a4eb02d9befbd1538e8d230699d4e434cdbee,	'Footprint Analytics',	'Season 3')
,('Project',	0x17c2d875cb397d813eae817dafd25807e348df07,	'Herodotus',	'Season 3')
,('Project',	0x37c752aa952cfb62cc403fd810f2a01801675bcc,	'Infinity Keys',	'Season 3')
,('Project',	0x7878aefb660af2b0928b6e1a8acd2b97ecb37c2c,	'Inverter',	'Season 3')
,('Project',	0xdd347ad6aa6d26ae6840ed3574e6ce4b0e002066,	'JiffyScan',	'Season 3')
,('Project',	0x8b6c27ec466923fad66ada94c78aa320ea876969,	'Opti.domains',	'Season 3')
,('Project',	0x7b57c388e6149b5c197b925037602d5b6baffbcc,	'Synpress',	'Season 3')
,('Project',	0x93501fddb36ea67ffa2c361b3da035c142299d4f,	'Another World',	'Season 3')
,('Project',	0x99a9cc2aaf54f6be94a437641692a72810f6e9d5,	'Bored Town',	'Season 3')
,('Project',	0x69c1a51711b061e5935c648beb16e349898b17df,	'dForce',	'Season 3')
,('Project',	0x926e2d35053e2d9d1cc72c6eda23f13a33270a76,	'Fairmint',	'Season 3')
,('Project',	0xde798cd9c53f4806b9cc7dd27adf7c641540167c,	'Geo Web',	'Season 3')
,('Project',	0x4d9339dd97db55e3b9bcbe65de39ff9c04d1c2cd,	'Giveth',	'Season 3')
,('Project',	0xb0e9021096d7d6458d747cfe959e5e066fc52ded,	'Metalswap',	'Season 3')
,('Project',	0x5ab0fdc6f0ca9ea178c95ceccf944a4ce1aedddd,	'Galxe',	'Season 3')
,('Project',	0x14ff422f5bacc52e20be81b184f4ce1f0367402b,	'Optiland Quests',	'Season 3')
,('Project',	0x5b5ecfc8122ba166b21d6ea26268ef97e09b2e9f,	'Paytrie',	'Season 3')
,('Project',	0xbd72a3cd66b3e40e5151b153164905fd65b55145,	'Rabbit Hole',	'Season 3')
,('Project',	0x6268d07327f4fb7380732dc6d63d95f88c0e083b,	'ShapeShift',	'Season 3')
,('Project',	0x7fb50bbabedee52b8760ba15c0c199af33fc2efa,	'Threshold',	'Season 3')
,('Project',	0x76d266dfd3754f090488ae12f6bd115cd7e77ebd,	'Vesper',	'Season 3')
-- Announced Partner Fund
,('Project',	0xfb0dadb835fade151abf6780beafb12fc5ba0e1f,	'PoolTogether',	'Partner Fund')
,('Project',	0x30688fcd63d0f2d067a7fe264b3367c348e9b031,	'Velodrome',	'Partner Fund')
,('Project',	0x13838884271ec954cb036d0b29d404afab5eae2a,	'CryptoTesters',	'Partner Fund')
,('Project',	0xeb952357394b6668684a4890bd49d8a4c3e19ec0,	'Okex',	'Partner Fund')
,('Project',	0xb1ea5a3e5ea7fa1834d48058ecda26d8c59e8251,	'Quix',	'Partner Fund')
,('Project',	0xf42279467d821bcdf40b50e9a5d2caccc4cf5b30,	'Quests on Coinbase Wallet - Quest #1 DEX Swap',	'Partner Fund') --Quest 1 - DEX
,('Project',	0x9f4f2b8bda8d2d3832021b3119747470ea86a183,	'Quests on Coinbase Wallet - Quest #2 Delegation',	'Partner Fund') --Quest 2 - Delegation
,('Project',	0x1fe95e0497a0e38afbe18bd19b9a2b42116880f0,	'Quests on Coinbase Wallet - Quest #3 Attestation',	'Partner Fund') --Quest 2 - Attestation
,('Project',	0x23fd464e0b0ee21cedeb929b19cabf9bd5215019,	'Exactly',	'Partner Fund')
,('Project',	0x91ce2f083d59b832f95f90aa0997168ae051a98a,	'Lido',		'Partner Fund')
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
