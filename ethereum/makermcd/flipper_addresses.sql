CREATE OR REPLACE VIEW makermcd.flipper_addresses (symbol, project, contract_address, underlying_token_address) AS VALUES
    ('MakerDAO'::text, 'WETH'::text,   '\xF32836B9E1f47a0515c6Ec431592D5EbC276407f'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'WETH'::text,   '\xF04a5cC80B1E94C69B48f5ee68a08CD2F09A7c3E'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --v
    ('MakerDAO'::text, 'WETH'::text,   '\x7A67901A68243241EBf66beEB0e7b5395582BF17'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'BAT'::text,   '\xF7C569B2B271354179AaCC9fF1e42390983110BA'::bytea, '\x0d8775f648430679a709e98d2b0cb6250d2887ef'::bytea), --
    ('MakerDAO'::text, 'USDC'::text,   '\xbe359e53038E41a1ffA47DAE39645756C80e557a'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea), --
    ('MakerDAO'::text, 'USDC'::text,   '\x77282aD36aADAfC16bCA42c865c674F108c4a616'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea), --
    ('MakerDAO'::text, 'TUSD'::text,   '\x9E4b213C4defbce7564F2Ac20B6E3bF40954C440'::bytea, '\x0000000000085d4780B73119b644AE5ecd22b376'::bytea), --
    ('MakerDAO'::text, 'ZRX'::text,   '\xa4341cAf9F9F098ecb20fb2CeE2a0b8C78A18118'::bytea, '\xe41d2489571d322189246dafa5ebde1f4699f498'::bytea), --
    ('MakerDAO'::text, 'KNC'::text,   '\x57B01F1B3C59e2C0bdfF3EC9563B71EEc99a3f2f'::bytea, '\xdd974d5c2e2928dea5f71b9825b8b646686bd200'::bytea), --
    ('MakerDAO'::text, 'Mana'::text,   '\x0a1D75B4f49BA80724a214599574080CD6B68357'::bytea, '\x0f5d2fb29fb7d3cfee444a200298f468908cc942'::bytea), --
    ('MakerDAO'::text, 'USDT'::text,   '\x667F41d0fDcE1945eE0f56A79dd6c142E37fCC26'::bytea, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea), --
    ('MakerDAO'::text, 'PAXUSD'::text,   '\x52D5D1C05CC79Fc24A629Cb24cB06C5BE5d766E7'::bytea, '\x8e870d67f660d95d5be530380d0ec0bd388289e1'::bytea), --
    ('MakerDAO'::text, 'COMP'::text,   '\x524826F84cB3A19B6593370a5889A58c00554739'::bytea, '\xc00e94cb662c3520282e6f5717214004a7f26888'::bytea), --
    ('MakerDAO'::text, 'LRC'::text,   '\x7FdDc36dcdC435D8F54FDCB3748adcbBF70f3dAC'::bytea, '\xbbbbca6a901c926f240b89eacb641d8aec7aeafd'::bytea), --
    ('MakerDAO'::text, 'LINK'::text,   '\xB907EEdD63a30A3381E6D898e5815Ee8c9fd2c85'::bytea, '\x514910771af9ca656af840dff83e8264ecf986ca'::bytea), --
    ('MakerDAO'::text, 'WBTC'::text,   '\x58CD24ac7322890382eE45A3E4F903a5B22Ee930'::bytea, '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea), --
    ('MakerDAO'::text, 'BAL'::text,   '\xb2b9bd446eE5e58036D2876fce62b7Ab7334583e'::bytea, '\xba100000625a3754423978a60c9317c58a424e3d'::bytea), --
    ('MakerDAO'::text, 'YFI'::text,   '\xEe4C9C36257afB8098059a4763A374a4ECFE28A7'::bytea, '\x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e'::bytea), --
    ('MakerDAO'::text, 'GUSD'::text,   '\xCAa8D152A8b98229fB77A213BE16b234cA4f612f'::bytea, '\x056Fd409E1d7A124BD7017459dFEa2F387b6d5Cd'::bytea), --
    ('MakerDAO'::text, 'UNI'::text,   '\xF5b8cD9dB5a0EC031304A7B815010aa7761BD426'::bytea, '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'::bytea), --
    ('MakerDAO'::text, 'RENBTC'::text,   '\x30BC6eBC27372e50606880a36B279240c0bA0758'::bytea, '\xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'::bytea), --
    ('MakerDAO'::text, 'AAVE'::text,   '\x16e1b844094c885a37509a8f76c533B5fbFED13a'::bytea, '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'::bytea), --
    ('MakerDAO'::text, 'UNIV2DAIUSDC'::text,   '\x4a613f79a250D522DdB53904D87b8f442EA94496'::bytea, '\xAE461cA67B15dc8dc81CE7615e0320dA1A9aB8D5'::bytea), --
    ('MakerDAO'::text, 'UNIV2DAIETH'::text,    '\x57dfd99f45747DD55C1c432Db4aEa07FBd5d2B5c'::bytea, '\xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11'::bytea), --
    ('MakerDAO'::text, 'UNIV2WBTCETH'::text,   '\xbc95e8904d879F371Ac6B749727a0EAfDCd2ACB6'::bytea, '\xBb2b8038a1640196FbE3e38816F3e67Cba72D940'::bytea), --
    ('MakerDAO'::text, 'UNIV2USDCETH'::text,   '\x48d2C08b93E57701C8ae8974Fc4ADd725222B0BB'::bytea, '\xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'::bytea), --
    ('MakerDAO'::text, 'UNIV2WBTCDAI'::text,   '\x172200d12D09C2698Dd918d347155fE6692f5662'::bytea, '\x231B7589426Ffe1b75405526fC32aC09D44364c4'::bytea), --
    ('MakerDAO'::text, 'UNIV2LINKETH'::text,   '\xb79f818E3c73FCA387845f892356224CA75eac4b'::bytea, '\xa2107FA5B38d9bbd2C461D6EDf11B11A50F6b974'::bytea), --
    ('MakerDAO'::text, 'UNIV2DAIUSDT'::text,   '\xD32f8B8aDbE331eC0CfADa9cfDbc537619622cFe'::bytea, '\xB20bd5D04BE54f870D5C0d3cA85d82b34B836405'::bytea), --
    ('MakerDAO'::text, 'UNIV2UNIETH'::text,    '\xe5ED7da0483e291485011D5372F3BF46235EB277'::bytea, '\xd3d2E2692501A5c9Ca623199D38826e513033a17'::bytea), --
    ('MakerDAO'::text, 'UNIV2ETHUSDT'::text,   '\x118d5051e70F9EaF3B4a6a11F765185A2Ca0802E'::bytea, '\x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852'::bytea), --
    ('MakerDAO'::text, 'UNIV2AAVEETH'::text,   '\x20D298ca96bf8c2000203B911908DbDc1a8Bac58'::bytea, '\xDFC14d2Af169B0D36C4EFF567Ada9b2E0CAE044f'::bytea)
;   
