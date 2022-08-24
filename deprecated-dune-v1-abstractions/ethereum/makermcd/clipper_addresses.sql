CREATE OR REPLACE VIEW makermcd.clipper_addresses (symbol, project, contract_address, underlying_token_address) AS VALUES
    ('MakerDAO'::text, 'WETH'::text,   '\xc67963a226eddd77B91aD8c421630A1b0AdFF270'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'WETH'::text,   '\x71eb894330e8a4b96b8d6056962e7F116F50e06F'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --v
    ('MakerDAO'::text, 'WETH'::text,   '\xc2b12567523e3f3CBd9931492b91fe65b240bc47'::bytea, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea), --
    ('MakerDAO'::text, 'BAT'::text,   '\x3D22e6f643e2F4c563fD9db22b229Cbb0Cd570fb'::bytea, '\x0d8775f648430679a709e98d2b0cb6250d2887ef'::bytea), --
    ('MakerDAO'::text, 'USDC'::text,   '\x046b1A5718da6A226D912cFd306BA19980772908'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea), --
    ('MakerDAO'::text, 'USDC'::text,   '\x5590F23358Fe17361d7E4E4f91219145D8cCfCb3'::bytea, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea), --
    ('MakerDAO'::text, 'TUSD'::text,   '\x0F6f88f8A4b918584E3539182793a0C276097f44'::bytea, '\x0000000000085d4780B73119b644AE5ecd22b376'::bytea), --
    ('MakerDAO'::text, 'ZRX'::text,   '\xdc90d461E148552387f3aB3EBEE0Bdc58Aa16375'::bytea, '\xe41d2489571d322189246dafa5ebde1f4699f498'::bytea), --
    ('MakerDAO'::text, 'KNC'::text,   '\x006Aa3eB5E666D8E006aa647D4afAB212555Ddea'::bytea, '\xdd974d5c2e2928dea5f71b9825b8b646686bd200'::bytea), --
    ('MakerDAO'::text, 'Mana'::text,   '\xF5C8176E1eB0915359E46DEd16E52C071Bb435c0'::bytea, '\x0f5d2fb29fb7d3cfee444a200298f468908cc942'::bytea), --
    ('MakerDAO'::text, 'USDT'::text,   '\xFC9D6Dd08BEE324A5A8B557d2854B9c36c2AeC5d'::bytea, '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea), --
    ('MakerDAO'::text, 'PAXUSD'::text,   '\xBCb396Cd139D1116BD89562B49b9D1d6c25378B0'::bytea, '\x8e870d67f660d95d5be530380d0ec0bd388289e1'::bytea), --
    ('MakerDAO'::text, 'COMP'::text,   '\x2Bb690931407DCA7ecE84753EA931ffd304f0F38'::bytea, '\xc00e94cb662c3520282e6f5717214004a7f26888'::bytea), --
    ('MakerDAO'::text, 'LRC'::text,   '\x81C5CDf4817DBf75C7F08B8A1cdaB05c9B3f70F7'::bytea, '\xbbbbca6a901c926f240b89eacb641d8aec7aeafd'::bytea), --
    ('MakerDAO'::text, 'LINK'::text,   '\x832Dd5f17B30078a5E46Fdb8130A68cBc4a74dC0'::bytea, '\x514910771af9ca656af840dff83e8264ecf986ca'::bytea), --
    ('MakerDAO'::text, 'WBTC'::text,   '\x0227b54AdbFAEec5f1eD1dFa11f54dcff9076e2C'::bytea, '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea), --
    ('MakerDAO'::text, 'BAL'::text,   '\x6AAc067bb903E633A422dE7BE9355E62B3CE0378'::bytea, '\xba100000625a3754423978a60c9317c58a424e3d'::bytea), --
    ('MakerDAO'::text, 'YFI'::text,   '\x9daCc11dcD0aa13386D295eAeeBBd38130897E6f'::bytea, '\x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e'::bytea), --
    ('MakerDAO'::text, 'GUSD'::text,   '\xa47D68b9dB0A0361284fA04BA40623fcBd1a263E'::bytea, '\x056Fd409E1d7A124BD7017459dFEa2F387b6d5Cd'::bytea), --
    ('MakerDAO'::text, 'UNI'::text,   '\x3713F83Ee6D138Ce191294C131148176015bC29a'::bytea, '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'::bytea), --
    ('MakerDAO'::text, 'RENBTC'::text,   '\x834719BEa8da68c46484E001143bDDe29370a6A3'::bytea, '\xEB4C2781e4ebA804CE9a9803C67d0893436bB27D'::bytea), --
    ('MakerDAO'::text, 'AAVE'::text,   '\x8723b74F598DE2ea49747de5896f9034CC09349e'::bytea, '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'::bytea), --
    ('MakerDAO'::text,'MATIC'::text,   '\x29342F530ed6120BDB219D602DaFD584676293d1'::bytea, '\x7d1afa7b718fb893db30a3abc0cfc608aacfebb0'::bytea), --
    ('MakerDAO'::text, 'WSTETH'::text,   '\x49A33A28C4C7D9576ab28898F4C9ac7e52EA457A'::bytea, '\x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'::bytea), --
    ('MakerDAO'::text, 'UNIV2DAIUSDC'::text,   '\x9B3310708af333f6F379FA42a5d09CBAA10ab309'::bytea, '\xAE461cA67B15dc8dc81CE7615e0320dA1A9aB8D5'::bytea), --
    ('MakerDAO'::text, 'UNIV2DAIETH'::text,   '\x9F6981bA5c77211A34B76c6385c0f6FA10414035'::bytea, '\xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11'::bytea), --
    ('MakerDAO'::text, 'UNIV2WBTCETH'::text,   '\xb15afaB996904170f87a64Fe42db0b64a6F75d24'::bytea, '\xBb2b8038a1640196FbE3e38816F3e67Cba72D940'::bytea), --
    ('MakerDAO'::text, 'UNIV2USDCETH'::text,   '\x93AE03815BAF1F19d7F18D9116E4b637cc32A131'::bytea, '\xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'::bytea), --
    ('MakerDAO'::text, 'UNIV2WBTCDAI'::text,   '\x4fC53a57262B87ABDa61d6d0DB2bE7E9BE68F6b8'::bytea, '\x231B7589426Ffe1b75405526fC32aC09D44364c4'::bytea), --
    ('MakerDAO'::text, 'UNIV2LINKETH'::text,   '\x6aa0520354d1b84e1C6ABFE64a708939529b619e'::bytea, '\xa2107FA5B38d9bbd2C461D6EDf11B11A50F6b974'::bytea), --
    ('MakerDAO'::text, 'UNIV2DAIUSDT'::text,   '\xe4B82Be84391b9e7c56a1fC821f47569B364dd4a'::bytea, '\xB20bd5D04BE54f870D5C0d3cA85d82b34B836405'::bytea), --
    ('MakerDAO'::text, 'GUNIV3DAIUSDC1'::text,   '\x5048c5Cd3102026472f8914557A1FD35c8Dc6c9e'::bytea, '\x6c6Bc977E13Df9b0de53b251522280BB72383700'::bytea), --
    ('MakerDAO'::text, 'UNIV2UNIETH'::text,   '\xb0ece6F5542A4577E2f1Be491A937Ccbbec8479e'::bytea, '\xd3d2E2692501A5c9Ca623199D38826e513033a17'::bytea), --
    ('MakerDAO'::text, 'UNIV2ETHUSDT'::text,   '\x2aC4C9b49051275AcB4C43Ec973082388D015D48'::bytea, '\x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852'::bytea), --
    ('MakerDAO'::text, 'UNIV2AAVEETH'::text,   '\x854b252BA15eaFA4d1609D3B98e00cc10084Ec55'::bytea, '\xDFC14d2Af169B0D36C4EFF567Ada9b2E0CAE044f'::bytea) --
;   
