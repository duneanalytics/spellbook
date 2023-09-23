{{
    config(
        schema = 'oneinch',
        alias = alias('exchange_contracts'),
        materialized = 'table',
        file_format = 'delta',
        unique_key = ['contract_address', 'blockchain'],
        tags = ['dunesql']
    )
}}



with 
    
contracts as (
    select
        project
        , address
        , contract_name
        , blockchain
    from (values
          ('1inch'				, 0xe4c577bdec9ce0f6c54f2f82aed5b1913b71ae2f, 'exchangeV1'					, array['ethereum'])
        , ('1inch'				, 0x0000000006adbd7c01bc0738cdbfc3932600ad63, 'exchangeV2'					, array['ethereum'])
        , ('1inch'				, 0x0000000053d411becdb4a82d8603edc6d8b8b3bc, 'exchangeV3'					, array['ethereum'])
        , ('1inch'				, 0x000005edbbc1f258302add96b5e20d3442e5dd89, 'exchangeV4'					, array['ethereum'])
        , ('1inch'				, 0x0000000f8ef4be2b7aed6724e893c1b674b9682d, 'exchangeV5'					, array['ethereum'])
        , ('1inch'				, 0x111112549cfedf7822eb11fbd8fd485d8a10f93f, 'exchangeV6'					, array['ethereum'])
        , ('1inch'				, 0x111111254b08ceeee8ad6ca827de9952d2a46781, 'exchangeV7'					, array['ethereum'])
        , ('1inch'              , 0x3ef51736315f52d568d6d2cf289419b9cfffe782, 'LimitOrderProtocolV1'        , array['ethereum'])
        , ('1inch'              , 0xe3456f4ee65e745a44ec3bcb83d0f2529d1b84eb, 'LimitOrderProtocolV1'        , array['bnb'])
        , ('1inch'              , 0xb707d89d29c189421163515c59e42147371d6857, 'LimitOrderProtocolV1'        , array['polygon', 'optimism'])
        , ('1inch'              , 0xe295ad71242373c37c5fda7b57f26f9ea1088afe, 'LimitOrderProtocolV1'        , array['arbitrum'])
        , ('1inch'              , 0x119c71d3bbac22029622cbaec24854d3d32d2828, 'LimitOrderProtocolV2'        , array['ethereum'])
        , ('1inch'              , 0x1e38eff998df9d3669e32f4ff400031385bf6362, 'LimitOrderProtocolV2'        , array['bnb'])
        , ('1inch'              , 0x94bc2a1c732bcad7343b25af48385fe76e08734f, 'LimitOrderProtocolV2'        , array['polygon'])
        , ('1inch'              , 0x54431918cec22932fcf97e54769f4e00f646690f, 'LimitOrderProtocolV2'        , array['gnosis'])
        , ('1inch'              , 0x7f069df72b7a39bce9806e3afaf579e54d8cf2b9, 'LimitOrderProtocolV2'        , array['arbitrum'])
        , ('1inch'              , 0x0f85a912448279111694f4ba4f85dc641c54b594, 'LimitOrderProtocolV2'        , array['avalanche_c'])
        , ('1inch'              , 0x11431a89893025d2a48dca4eddc396f8c8117187, 'LimitOrderProtocolV2'        , array['optimism'])
        , ('1inch'				, 0x11111254369792b2ca5d084ab5eea397ca8fa48b, 'AggregationRouterV1'			, array['ethereum'])
        , ('1inch'				, 0x111111125434b319222cdbf8c261674adb56f3ae, 'AggregationRouterV2'			, array['ethereum'])
        , ('1inch'				, 0x111111254bf8547e7183e4bbfc36199f3cedf4a1, 'AggregationRouterV2'			, array['bnb'])
        , ('1inch'				, 0x11111112542d85b3ef69ae05771c2dccff4faa26, 'AggregationRouterV3'			, array['ethereum', 'bnb', 'polygon', 'optimism', 'arbitrum'])
        , ('1inch'				, 0x1111111254fb6c44bac0bed2854e76f90643097d, 'AggregationRouterV4'			, array['ethereum', 'bnb', 'polygon', 'arbitrum', 'avalanche_c', 'gnosis', 'fantom'])
        , ('1inch'				, 0x1111111254760f7ab3f16433eea9304126dcd199, 'AggregationRouterV4'			, array['optimism'])
        , ('1inch'				, 0x1111111254eeb25477b68fb85ed929f73a960582, 'AggregationRouterV5'			, array['ethereum', 'bnb', 'polygon', 'optimism', 'arbitrum', 'avalanche_c', 'gnosis', 'fantom', 'base'])
        , ('AnySwap'			, 0x0cae51e1032e8461f4806e26332c030e34de3adb, 'AnyswapV3Router'				, array['arbitrum'])
        , ('AnySwap'			, 0x7782046601e7b9b05ca55a3899780ce6ee6b8b2b, 'AnyswapV6Router'				, array['ethereum'])
        , ('AnySwap'			, 0xb0731d50c681c45856bfc3f7539d5f61d4be81d8, ''							, array['avalanche_c'])
        , ('AnySwap'			, 0xe1d592c3322f1f714ca11f05b6bc0efef1907859, 'AnyswapV6Router'				, array['bnb'])
        , ('AnySwap'			, 0xf9736ec3926703e85c843fc972bd89a7f8e827c0, 'AnyswapV3Router'				, array['bnb'])
        , ('ApeSwap'			, 0xc0788a3ad43d79aa53b09c2eacc313a787d1d607, 'ApeRouter'					, array['polygon'])
        , ('ApeSwap'			, 0xcf0febd3f17cef5b47b0cd257acf6025c5bff3b7, ''							, array['bnb'])
        , ('Arbswap'			, 0x6947a425453d04305520e612f0cb2952e4d07d62, 'ArbswapSmartRouter'			, array['arbitrum'])
        , ('BabySwap'			, 0x8317c460c22a9958c27b4b6403b98d2ef4e2ad32, ''							, array['bnb'])
        , ('Baoswap'			, 0x6093aebac87d62b1a5a4ceec91204e35020e38be, 'UniswapV2Router02'			, array['gnosis'])
        , ('BiSwap'				, 0x3a6d8ca21d1cf76f653a67577fa0d27453350dd8, ''							, array['bnb'])
        , ('BitKeep'			, 0x6f5ac65ca70f2a44d73c8f711cb2bdf425d9f304, ''							, array['bnb'])
        , ('BitKeep'			, 0xf5bfcbda96de6a55a3a80af5175a1cbb088d5338, ''							, array['polygon'])
        , ('BitKeep'			, 0xf6463845b0b9d9d33d8e2bcb6c628bc5cb1ad133, ''							, array['polygon'])
        , ('Bogged Finance'		, 0xb099ed146fad4d0daa31e3810591fc0554af62bb, 'BogSwap'						, array['bnb', 'avalanche_c', 'fantom'])
        , ('Camelot'			, 0xc873fecbd354f5a56e00e710b90ef4201db2448d, 'CamelotRouter'				, array['arbitrum'])
        , ('Clipper'			, 0x5130f6ce257b8f9bf7fac0a0b519bd588120ed40, 'PackedVerifiedExchange'		, array['optimism'])
        , ('CoW Swap'			, 0x9008d19f58aabd9ed0d60971565aa8510560ab41, 'GPv2Settlement'				, array['gnosis', 'ethereum'])
        , ('Cone'				, 0xbf1fc29668e5f5eaa819948599c9ac1b1e03e75f, 'ConeRouter01'				, array['bnb'])
        , ('Connext'			, 0x11984dc4465481512eb5b777e44061c158cf2259, 'ConnextDiamond'				, array['polygon'])
        , ('Connext'			, 0xee9dec2712cce65174b561151701bf54b99c24c8, 'ConnextDiamond'				, array['arbitrum'])
        , ('Curvefi'			, 0x99a58482bd75cbab83b27ec03ca68ff489b5788f, 'SwapRouter'					, array['ethereum'])
        , ('Curvefi'			, 0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7, 'ThreepoolSwap'				, array['ethereum'])
        , ('Curvefi'			, 0xdc24316b9ae028f1497c275eb9192a3ea0f67022, 'StETHSwap'					, array['ethereum'])
        , ('Curvefi'			, 0x1d8b86e3d88cdb2d34688e87e72f388cb541b7c8, 'VyperContract'				, array['polygon'])
        , ('DODO'				, 0x0656fd85364d03b103ceeda192fb2d3906a6ac15, 'DODOFeeRouteProxy'			, array['bnb'])
        , ('DODO'				, 0x2fa4334cfd7c56a0e7ca02bd81455205fcbdc5e9, 'DODORouteProxy'				, array['polygon'])
        , ('DODO'				, 0x39e3e49c99834c9573c9fc7ff5a4b226cd7b0e63, 'DODOFeeRouteProxy'			, array['polygon'])
        , ('DODO'				, 0x3b6067d4caa8a14c63fdbe6318f27a0bbc9f9237, 'DODORouteProxy'				, array['arbitrum'])
        , ('DODO'				, 0x6b3d817814eabc984d51896b1015c0b89e9737ca, 'DODORouteProxy'				, array['bnb'])
        , ('DODO'				, 0xa222e6a71d1a1dd5f279805fbe38d5329c1d0e70, 'DODOV2Proxy02'				, array['polygon'])
        , ('DODO'				, 0xa2398842f37465f89540430bdc00219fa9e4d28a, 'DODORouteProxy'				, array['ethereum'])
        , ('DODO'				, 0xa8b034301bb5dd3610db585def3e7c0d52f2319f, 'DODOFeeRouteProxy'			, array['bnb'])
        , ('DODO'				, 0xe05dd51e4eb5636f4f0e8e7fbe82ea31a2ecef16, 'DODOFeeRouteProxy'			, array['arbitrum'])
        , ('Dfx Finance'		, 0x9d0950c595786aba7c26dfddf270d66a8b18b4fa, 'Router'						, array['ethereum'])
        , ('Dfyn'				, 0xa102072a4c07f06ec3b4900fdc4c7b80b6c57429, 'UniswapV2Router02'			, array['polygon'])
        , ('Dzap'				, 0x3af3cc4930ef88f4afe0b695ac95c230e1a108ec, 'DZapAggregator'				, array['polygon'])
        , ('Firebird'			, 0x0c6134abc08a1eafc3e2dc9a5ad023bb08da86c3, 'FireBirdRouter'				, array['optimism'])
        , ('Firebird'			, 0x92e4f29be975c1b1eb72e77de24dccf11432a5bd, 'FireBirdRouter'				, array['bnb'])
        , ('Firebird'			, 0xb31d1b1ea48ce4bf10ed697d44b747287e785ad4, 'FireBirdRouter'				, array['polygon'])
        , ('Firebird'			, 0xe0c38b2a8d09aad53f1c67734b9a95e43d5981c0, 'Firebird'					, array['fantom'])
        , ('Fraxswap'			, 0x9bc2152fd37b196c0ff3c16f5533767c9a983971, 'FraxswapRouter'				, array['polygon'])
        , ('FstSwap'			, 0x1b6c9c20693afde803b27f8782156c0f892abc2d, 'FstswapRouter02'				, array['bnb'])
        , ('FstSwap'			, 0xb3ca4d73b1e0ea2c53b42173388cc01e1c226f40, ''							, array['bnb'])
        , ('GMX'				, 0x3d6ba331e3d9702c5e8a8d254e5d8a285f223aba, 'PositionRouter'				, array['arbitrum'])
        , ('GMX'				, 0x5f719c2f1095f7b9fc68a68e35b51194f4b6abe8, 'Router'						, array['avalanche_c'])
        , ('GMX'				, 0xa27c20a7cf0e1c68c0460706bb674f98f362bc21, 'OrderBookReader'				, array['arbitrum'])
        , ('GMX'				, 0xabbc5f99639c9b6bcb58544ddf04efa6802f4064, 'Router'						, array['arbitrum'])
        , ('GemSwap'			, 0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2, 'GemSwap'						, array['ethereum'])
        , ('HoneySwap'			, 0x1c232f01118cb8b424793ae03f870aa7d0ac7f77, 'UniswapV2Router02'			, array['gnosis'])
        , ('Izumi'				, 0xbd3bd95529e0784ad973fd14928eedf3678cfad8, 'Swap'						, array['bnb'])
        , ('Kyber'				, 0x00555513acf282b42882420e5e5ba87b44d8fa6e, 'AggregationRouterV3'			, array['fantom'])
        , ('Kyber'				, 0x1fc3607fa67b58deddb0faf7a116f417a20c551c, 'AggregationRouter'			, array['fantom'])
        , ('Kyber'				, 0x546c79662e028b661dfb4767664d0273184e4dd1, 'AggregationRouter'			, array['polygon'])
        , ('Kyber'				, 0x6131b5fae19ea4f9d964eac0408e4408b66337b5, 'MetaAggregationRouterV2'		, array['ethereum', 'bnb', 'polygon', 'arbitrum', 'avalanche_c', 'fantom'])
        , ('Kyber'				, 0x617dee16b86534a5d792a4d7a62fb491b544111e, 'MetaAggregationRouterV1'		, array['fantom', 'ethereum', 'polygon'])
        , ('Kyber'				, 0xdf1a1b60f2d438842916c0adc43748768353ec25, 'AggregationRouterV2'			, array['fantom', 'polygon'])
        , ('Levinswap'			, 0xb18d4f69627f8320619a696202ad2c430cef7c53, 'UniswapV2Router02'			, array['gnosis'])
        , ('LiFi'				, 0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae, 'DiamondV2'					, array['bnb', 'polygon', 'optimism', 'arbitrum', 'avalanche_c', 'gnosis', 'fantom'])
        , ('Mdex'				, 0x62c1a0d92b09d0912f7bb9c96c5ecdc7f2b87059, 'MdexRouter'					, array['bnb'])
        , ('Mdex'				, 0x7dae51bd3e3376b8c7c4900e9107f12be3af1ba8, 'MdexRouter'					, array['bnb'])
        , ('Meshswap'			, 0x10f4a785f458bc144e3706575924889954946639, 'Router'						, array['polygon'])
        , ('MetaMask'			, 0x1a1ec25dc08e98e5e93f1104b5e5cdd298707d31, 'MetaSwap'					, array['bnb', 'polygon', 'avalanche_c'])
        , ('MetaMask'			, 0x881d40237659c251811cec9c364ef91dc08d300c, 'MetaSwap'					, array['ethereum'])
        , ('NomiSwap'			, 0xd654953d746f0b114d1f85332dc43446ac79413d, ''							, array['bnb'])
        , ('OKX DEX'			, 0x3b3ae790df4f312e745d270119c6052904fb6790, 'DEX'							, array['ethereum'])
        , ('Odos'				, 0x69dd38645f7457be13571a847ffd905f9acbaf6d, 'OdosRouter'					, array['optimism'])
        , ('Odos'				, 0x76f4eed9fe41262669d0250b2a97db79712ad855, 'Odos'						, array['ethereum'])
        , ('Odos'				, 0xa32ee1c40594249eb3183c10792bcf573d4da47c, ''							, array['polygon'])
        , ('Odos'				, 0xdd94018f54e565dbfc939f7c44a16e163faab331, 'OdosRouter'					, array['arbitrum'])
        , ('Odos'				, 0xfe7ce93ac0f78826cd81d506b07fe9f459c00214, ''							, array['avalanche_c'])
        , ('OpenOcean'			, 0x6352a56caadc4f1e25cd6c75970fa768a3304e64, 'ExchangeV2'					, array['bnb', 'avalanche_c', 'ethereum', 'optimism', 'fantom'])
        , ('PancakeSwap'		, 0x05ff2b0db69458a0750badebc4f9e13add608c7f, 'PancakeRouter'				, array['bnb'])
        , ('PancakeSwap'		, 0x10ed43c718714eb63d5aa57b78b54704e256024e, 'pancakeswap_v2'				, array['bnb'])
        , ('PancakeSwap'		, 0x13f4ea83d0bd40e75c8222255bc855a974568dd4, 'SmartRouter'					, array['ethereum', 'bnb'])
        , ('PancakeSwap'		, 0x2f22e47ca7c5e07f77785f616ceee80c5e84127c, 'SwapSmartRouter'				, array['bnb'])
        , ('PancakeSwap'		, 0xd4c4a7c55c9f7b3c48bafb6e8643ba79f42418df, 'ZapV1'						, array['bnb'])
        , ('Pangolin Exchange'	, 0xe54ca86531e17ef3616d22ca28b0d458b6c89106, 'Router'						, array['avalanche_c'])
        , ('Paraswap'			, 0x90249ed4d69d70e709ffcd8bee2c5a566f65dade, 'AugustusSwapperV4'			, array['polygon'])
        , ('Paraswap'			, 0xdef171fe48cf0115b1d80b88dc8eab59176fee57, 'AugustusSwapperV5'			, array['bnb', 'ethereum', 'fantom', 'optimism', 'arbitrum', 'polygon', 'avalanche_c'])
        , ('Perp'				, 0x82ac2ce43e33683c58be4cdc40975e73aa50f459, 'ClearingHouse'				, array['optimism'])
        , ('QuickSwap'			, 0x2cf7252e74036d1da831d11089d326296e64a728, 'UniswapV2Pair'				, array['polygon'])
        , ('QuickSwap'			, 0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff, 'UniswapV2Router02'			, array['polygon'])
        , ('QuickSwap'			, 0xf5b509bb0909a69b1c207e495f687a596c168e12, 'SwapRouter'					, array['polygon'])
        , ('Rainbow'			, 0x00000000009726632680fb29d3f7a9734e3010e2, 'RainbowRouter'				, array['ethereum'])
        , ('Ramses'				, 0xaaa87963efeb6f7e0a2711f397663105acb1805e, 'Router'						, array['arbitrum'])
        , ('SafeMoon'			, 0x37da632c6436137bd4d0ca30c98d3c615974120b, ''							, array['bnb'])
        , ('Saita'				, 0x0c17e776cd218252adfca8d4e761d3fe757e9778, 'SaitaSwapRouter'				, array['ethereum'])
        , ('ShibaSwap'			, 0x03f7724180aa6b939894b5ca4314783b0b36b329, 'UniswapV2Router02'			, array['ethereum'])
        , ('Slingshot Finance'	, 0x00c0184c0b5d42fba6b7ca914b31239b419ab80b, 'Swap'						, array['optimism'])
        , ('Slingshot Finance'	, 0x07e56b727e0eacfa53823977599905024c2de4f0, ''							, array['polygon'])
        , ('Slingshot Finance'	, 0x224b239b8bb896f125bd77eb334e302a318d9e33, 'Swap'						, array['bnb'])
        , ('Slingshot Finance'	, 0x5543550d65813c1fa76242227cbba0a28a297771, ''							, array['arbitrum'])
        , ('Slingshot Finance'	, 0xe8c97bf6d084880de38aec1a56d97ed9fdfa0c9b, 'Swap'						, array['arbitrum'])
        , ('Slingshot Finance'	, 0xf2e4209afa4c3c9eaa3fb8e12eed25d8f328171c, 'TradingContract'				, array['polygon'])
        , ('Socket'				, 0x2b42affd4b7c14d9b7c2579229495c052672ccd3, 'Registry'					, array['avalanche_c'])
        , ('Socket'				, 0xc30141b657f4216252dc59af2e7cdb9d8792e1b0, 'Registry'					, array['ethereum', 'polygon', 'optimism', 'arbitrum', 'gnosis'])
        , ('Solidlizard'		, 0xf26515d5482e2c2fd237149bf6a653da4794b3d0, 'Router'						, array['arbitrum'])
        , ('Solisnek'			, 0xeeee17b45e4d127cfaaad14e2710489523adb4d8, 'Router'						, array['avalanche_c'])
        , ('Spiritswap'			, 0x16327e3fbdaca3bcf7e38f5af2599d2ddc33ae52, 'Router'						, array['fantom'])
        , ('Spookyswap'			, 0x31f63a33141ffee63d4b26755430a390acdd8a4d, 'LiquidityBrewer'				, array['fantom'])
        , ('Spookyswap'			, 0xf491e7b69e4244ad4002bc14e878a34207e38c29, 'UniswapV2Router02'			, array['fantom'])
        , ('SushiSwap'			, 0x1b02da8cb0d097eb8d57a175b88c7d8b47997506, 'SwapRouter'					, array['gnosis', 'avalanche_c', 'fantom', 'bnb', 'polygon', 'arbitrum'])
        , ('SushiSwap'			, 0x2c8c987c4777ab740d20cb581f5d381be95a4a4a, 'SushiXSwap'					, array['avalanche_c'])
        , ('SushiSwap'			, 0x4c5d5234f232bd2d76b96aa33f5ae4fcf0e4bfab, 'RouteProcessorV3'			, array['optimism'])
        , ('SushiSwap'			, 0x7a4af156379f512de147ed3b96393047226d923f, 'SushiXSwap'					, array['bnb'])
        , ('SushiSwap'			, 0x8b396ddf906d552b2f98a8e7d743dd58cd0d920f, 'SushiXSwap'					, array['optimism'])
        , ('SushiSwap'			, 0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f, 'Router02'					, array['ethereum'])
        , ('Swapr'				, 0xb9960d9bca016e9748be75dd52f02188b9d0829f, 'Swapr'						, array['ethereum'])
        , ('Swapr'				, 0xe43e60736b1cb4a75ad25240e2f9a62bff65c0c0, 'DXswapRouter'				, array['gnosis'])
        , ('Swapr'				, 0x1342a24347532de79372283b3a29c63c31dd7711, 'V2Router02'					, array['arbitrum'])
        , ('Swftswap'			, 0x1ed5685f345b2fa564ea4a670de1fde39e484751, 'SwftSwap'					, array['bnb'])
        , ('Swftswap'			, 0x92e929d8b2c8430bcaf4cd87654789578bb2b786, 'SwftSwap'					, array['ethereum'])
        , ('Tokenlon'			, 0x03f34be1bf910116595db1b11e9d1b2ca5d59659, 'Tokenlon'					, array['ethereum'])
        , ('Trader Joe'			, 0x079c68167f85cb06ed550149cce250e06dc3c52d, 'SwapLogic'					, array['avalanche_c'])
        , ('Trader Joe'			, 0x60ae616a2155ee3d9a68541ba4544862310933d4, 'JoeRouter02'					, array['avalanche_c'])
        , ('Trader Joe'			, 0xb4315e873dbcf96ffd0acd8ea43f689d8c20fb30, 'LBRouter'					, array['bnb', 'arbitrum', 'avalanche_c'])
        , ('Trader Joe'			, 0xed8cbd9f0ce3c6986b22002f03c6475ceb7a6256, 'JoePair'						, array['avalanche_c'])
        , ('TransitSwap'		, 0x638f32fe09baec1fdc54f962e3e8e5f2b286aa70, 'XswapProxyV3'				, array['bnb'])
        , ('TransitSwap'		, 0x8b48715c5d6d1645663a4c460ea85ce846b8580e, ''							, array['polygon'])
        , ('TransitSwap'		, 0xb45a2dda996c32e93b8c47098e90ed0e7ab18e39, 'TransitSwapRouterV4'			, array['bnb', 'polygon', 'arbitrum'])
        , ('Uniswap'			, 0x4c60051384bd2d3c01bfc845cf5f4b44bcbe9de5, 'UniversalRouter'				, array['polygon', 'arbitrum'])
        , ('Uniswap'			, 0x5dc88340e1c5c6366864ee415d6034cadd1a9897, 'UniversalRouter'				, array['bnb'])
        , ('Uniswap'			, 0x6000da47483062a0d734ba3dc7576ce6a0b645c4, 'UniswapX'					, array['ethereum'])
        , ('Uniswap'			, 0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45, 'SwapRouter02'				, array['ethereum', 'polygon', 'optimism', 'arbitrum'])
        , ('Uniswap'			, 0x7a250d5630b4cf539739df2c5dacb4c659f2488d, 'Router02'					, array['ethereum'])
        , ('Uniswap'			, 0xb555edf5dcf85f42ceef1f3630a52a108e55a654, 'UniversalRouter'				, array['optimism'])
        , ('Uniswap'			, 0xe592427a0aece92de3edee1f18e0157c05861564, 'SwapRouter'					, array['ethereum', 'polygon', 'optimism'])
        , ('Uniswap'			, 0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b, 'UniversalRouter'				, array['ethereum'])
        , ('Uniswap'			, 0xf164fc0ec4e93095b804a4795bbe1e041497b92a, 'Router01'					, array['ethereum'])
        , ('Velodrome'			, 0x9c12939390052919af3155f41bf4160fd3666a6f, 'Router'						, array['optimism'])
        , ('Velodrome'			, 0xa132dab612db5cb9fc9ac426a0cc215a3423f9c9, 'Router'						, array['optimism'])
        , ('Via router'			, 0x777777773fdd8b28bb03377d10fcea75ad9768da, 'ViaRouter'					, array['polygon', 'optimism', 'arbitrum'])
        , ('W3swap'				, 0xf29ace1fe5f36389d0dde450a0195a30c3770245, 'W3swapRouterV2'				, array['bnb'])
        , ('Wigoswap'			, 0x5023882f4d1ec10544fcb2066abe9c1645e95aa0, 'Router'						, array['fantom'])
        , ('Wombat'				, 0x19609b03c976cca288fbdae5c21d4290e9a4add7, 'WombatRouter'				, array['bnb'])
        , ('WooFi'				, 0x5aa6a4e96a9129562e2fc06660d07feddaaf7854, 'WooRouterV2'					, array['avalanche_c'])
        , ('WooFi'				, 0xcef5be73ae943b77f9bc08859367d923c030a269, 'WooRouterV2'					, array['bnb'])
        , ('WooFi'				, 0xdf37f7a85d4563f39a78494568824b4df8669b7a, 'WooCrossChainRouter'			, array['avalanche_c'])
        , ('WooFi'				, 0xeaf1ac8e89ea0ae13e0f03634a4ff23502527024, 'WooRouter'					, array['optimism'])
        , ('Yield'				, 0xc4729e56b831d74bbc18797e0e17a295fa77488c, 'YakRouter'					, array['avalanche_c'])
        , ('ZeroEx'				, 0x3f93c3d9304a70c9104642ab8cd37b1e2a7c203a, 'Exchange'					, array['bnb'])
        , ('ZeroEx'				, 0xdef189deaef76e379df891899eb5a00a94cbc250, 'ExchangeProxy'				, array['fantom'])
        , ('ZeroEx'				, 0xdef1abe32c034e558cdd535791643c58a13acc10, 'ExchangeProxy'				, array['optimism'])
        , ('ZeroEx'				, 0xdef1c0ded9bec7f1a1670819833240f027b25eff, 'ExchangeProxy'				, array['ethereum', 'bnb', 'polygon', 'arbitrum'])
        , ('ZeroEx'				, 0xe66b31678d6c16e9ebf358268a790b763c133750, 'ExchangeProxy'				, array['ethereum'])
    ) as c(project, address, contract_name, blockchains), unnest(blockchains) as blockchains(blockchain)
)

, descriptions as (
    select
        blockchain
        , address
        , max_by(abi, created_at) as abi
        , max_by(map_from_entries(array[
            ('dynamic', cast(dynamic as varchar))
            , ('base', cast(base as varchar))
            , ('factory', cast(factory as varchar))
            , ('detection_source', detection_source)
        ]), created_at) as params
        , array_agg(namespace) as namespaces
        , array_agg(name) as names
    from {{ ref('evms_contracts') }}
    join contracts using(blockchain, address)
    group by 1, 2
)

, creations as (
    
    select
        project
        , address
        , contract_name
        , blockchain
        , block_time as created_at
        , creation_traces."from" as creator
        , tx_hash as creation_tx_hash
        , abi
        , params
        , namespaces
        , names
    from {{ ref('evms_creation_traces') }}
    join contracts using(blockchain, address)
    left join descriptions using(blockchain, address)

)

select
      project
    , address as contract_address
    , substr(address, length(address) - 1) as contract_id
    , contract_name
    , blockchain
    , created_at
    , creator
    , creation_tx_hash
    , abi
    , params
    , namespaces
    , names
from creations
order by project, created_at