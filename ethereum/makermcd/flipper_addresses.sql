CREATE OR REPLACE VIEW makermcd.flipper_addresses (symbol, project, contract_address, underlying_token_address) AS VALUES
    ('MakerDAO'::text, 'WETH'::text,   '\xF32836B9E1f47a0515c6Ec431592D5EbC276407f'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'WETH'::text,   '\xF04a5cC80B1E94C69B48f5ee68a08CD2F09A7c3E'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --v
    ('MakerDAO'::text, 'WETH'::text,   '\x7A67901A68243241EBf66beEB0e7b5395582BF17'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'BAT'::text,   '\xF7C569B2B271354179AaCC9fF1e42390983110BA'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'USDC'::text,   '\xbe359e53038E41a1ffA47DAE39645756C80e557a'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'USDC'::text,   '\x77282aD36aADAfC16bCA42c865c674F108c4a616'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'TUSD'::text,   '\x9E4b213C4defbce7564F2Ac20B6E3bF40954C440'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'ZRX'::text,   '\xa4341cAf9F9F098ecb20fb2CeE2a0b8C78A18118'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'KNC'::text,   '\x57B01F1B3C59e2C0bdfF3EC9563B71EEc99a3f2f'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'Mana'::text,   '\x0a1D75B4f49BA80724a214599574080CD6B68357'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'USDT'::text,   '\x667F41d0fDcE1945eE0f56A79dd6c142E37fCC26'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'PAXUSD'::text,   '\x52D5D1C05CC79Fc24A629Cb24cB06C5BE5d766E7'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'COMP'::text,   '\x524826F84cB3A19B6593370a5889A58c00554739'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'LRC'::text,   '\x7FdDc36dcdC435D8F54FDCB3748adcbBF70f3dAC'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'LINK'::text,   '\xB907EEdD63a30A3381E6D898e5815Ee8c9fd2c85'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'WBTC'::text,   '\x58CD24ac7322890382eE45A3E4F903a5B22Ee930'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'BAL'::text,   '\xb2b9bd446eE5e58036D2876fce62b7Ab7334583e'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'YFI'::text,   '\xEe4C9C36257afB8098059a4763A374a4ECFE28A7'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'GUSD'::text,   '\xCAa8D152A8b98229fB77A213BE16b234cA4f612f'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNI'::text,   '\xF5b8cD9dB5a0EC031304A7B815010aa7761BD426'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'RENBTC'::text,   '\x30BC6eBC27372e50606880a36B279240c0bA0758'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'AAVE'::text,   '\x16e1b844094c885a37509a8f76c533B5fbFED13a'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNIV2DAIUSDC'::text,   '\x4a613f79a250D522DdB53904D87b8f442EA94496'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNIV2DAIETH'::text,   '\x57dfd99f45747DD55C1c432Db4aEa07FBd5d2B5c'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNIV2WBTCETH'::text,   '\xbc95e8904d879F371Ac6B749727a0EAfDCd2ACB6'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNIV2USDCETH'::text,   '\x48d2C08b93E57701C8ae8974Fc4ADd725222B0BB'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNIV2WBTCDAI'::text,   '\x172200d12D09C2698Dd918d347155fE6692f5662'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNIV2LINKETH'::text,   '\xb79f818E3c73FCA387845f892356224CA75eac4b'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNIV2DAIUSDT'::text,   '\xD32f8B8aDbE331eC0CfADa9cfDbc537619622cFe'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNIV2UNIETH'::text,   '\xe5ED7da0483e291485011D5372F3BF46235EB277'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNIV2ETHUSDT'::text,   '\x118d5051e70F9EaF3B4a6a11F765185A2Ca0802E'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'UNIV2AAVEETH'::text,   '\x20D298ca96bf8c2000203B911908DbDc1a8Bac58'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea)
;   
