{{ config(
    alias = alias('pool_details')
) }}

SELECT
    version,
    `name`,
    symbol,
    lower(pool_address) AS pool_address,
    lower(token_address) AS token_address,
    lower(gauge_contract) AS gauge_contract,
    lower(coin0) AS coin0,
    lower(coin1) AS coin1,
    lower(coin2) AS coin2,
    lower(undercoin0) AS undercoin0,
    lower(undercoin1) AS undercoin1,
    lower(undercoin2) AS undercoin2,
    lower(undercoin3) AS undercoin3,
    lower(deposit_contract) AS deposit_contract,
    lower(coin3) AS coin3
FROM
    (
        VALUES
            (
                'Regular',
                '3pool',
                '3pool',
                '0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7',
                '0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490',
                '0xbFcF63294aD7105dEa65aA58F8AE5BE2D9d0952A',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'aave',
                'aave',
                '0xDeBF20617708857ebe4F679508E7b7863a8A8Eee',
                '0xFd2a8fA60Abd58Efe3EeE34dd494cD491dC14900',
                '0xd662908ADA2Ea1916B3318327A97eB18aD588b5d',
                '0x028171bca77440897b824ca71d1c56cac55b68a3',
                '0xbcca60bb61934080951369a648fb03df4f96263c',
                '0x3ed3b47dd13ec9a98b44e6204a523e766b225811',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'ankreth',
                'ankreth',
                '0xA96A65c051bF88B4095Ee1f2451C2A9d43F53Ae2',
                '0xaA17A236F2bAdc98DDc0Cf999AbB47D47Fc0A6Cf',
                '0x6d10ed2cF043E6fcf51A0e7b4C2Af3Fa06695707',
                '0x0000000000000000000000000000000000000000',
                '0xe95a203b1a91a908f9b9ce46459d101078c2c3cB',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'bbtc',
                'bbtc',
                '0x071c661B4DeefB59E2a3DdB20Db036821eeE8F4B',
                '0x410e3E86ef427e30B9235497143881f717d93c2A',
                '0xdFc7AdFa664b08767b735dE28f9E84cd30492aee',
                '0x9be89d2a4cd102d8fecc6bf9da793be995c22541',
                '0x075b1bb99792c9e1041ba13afef80c91a1e70fb3',
                NULL,
                '0x9be89d2a4cd102d8fecc6bf9da793be995c22541',
                '0xeb4c2781e4eba804ce9a9803c67d0893436bb27d',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                '0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6',
                '0xC45b2EEe6e09cA176Ca3bB5f7eEe7C47bF93c756',
                NULL
            ),
            (
                'Regular',
                'busd',
                'busd',
                '0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27',
                '0x3B3Ac5386837Dc563660FB6a0937DFAa5924333B',
                '0x69Fb7c45726cfE2baDeE8317005d3F94bE838840',
                '0xc2cb1040220768554cf699b0d863a3cd4324ce32',
                '0x26ea744e5b887e5205727f55dfbe8685e3b21951',
                '0xe6354ed5bc4b393a5aad09f21c46e101e692d447',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x4fabb145d64652a948d72533023f6e7a623c7c53',
                '0xb6c057591E073249F2D9D88Ba59a46CFC9B59EdB',
                '0x04bc0ab673d88ae9dbc9da2380cb6b79c4bca9ae'
            ),
            (
                'Regular',
                'compound',
                'compound',
                '0xA2B47E3D5c44877cca798226B7B8118F9BFb7A56',
                '0x845838DF265Dcd2c412A1Dc9e959c7d08537f8a2',
                '0x7ca5b0a2910B33e9759DC7dDB0413949071D7575',
                '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643',
                '0x39aa39c021dfbae8fac545936693ac917d5e7563',
                NULL,
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                NULL,
                NULL,
                '0xeB21209ae4C2c9FF2a86ACA31E123764A3B6Bc06',
                NULL
            ),
            (
                'Regular',
                'crveth',
                'crveth',
                '0x8301AE4fc9c624d1D396cbDAa1ed877821D7C511',
                '0xEd4064f376cB8d68F770FB1Ff088a3d0F3FF5c4d',
                '0x1cEBdB0856dd985fAe9b8fEa2262469360B8a3a6',
                '0x0000000000000000000000000000000000000000',
                '0xd533a949740bb3306d119cc777fa900ba034cd52',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'cvxeth',
                'cvxeth',
                '0xB576491F1E6e5E62f1d8F26062Ee822B40B0E0d4',
                '0x3A283D9c08E8b55966afb64C515f5143cf907611',
                '0x7E1444BA99dcdFfE8fBdb42C02F0005D14f13BE1',
                '0x0000000000000000000000000000000000000000',
                '0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2B',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'dusd',
                'dusd',
                '0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c',
                '0x3a664Ab939FD8482048609f652f9a0B0677337B9',
                '0xAEA6c312f4b3E04D752946d329693F7293bC2e6d',
                '0x5bc25f649fc4e26069ddf4cf4010f9f706c23831',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0x5bc25f649fc4e26069ddf4cf4010f9f706c23831',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x61E10659fe3aa93d036d099405224E4Ac24996d0',
                NULL
            ),
            (
                'Regular',
                'eurs',
                'eurs',
                '0x0Ce6a5fF5217e38315f87032CF90686C96627CAA',
                '0x194eBd173F6cDacE046C53eACcE9B953F28411d1',
                '0x90Bb609649E0451E5aD952683D64BD2d1f245840',
                '0xdb25f211ab05b1c97d595516f45794528a807ad8',
                '0xd71ecff9342a5ced620049e616c5035f1db98620',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'eursusd',
                'eursusd',
                '0x98a7F18d4E56Cfe84E3D081B40001B3d5bD3eB8B',
                '0x3d229e1b4faab62f621ef2f6a610961f7bd7b23B',
                '0x65CA7Dc5CB661fC58De57B1E1aF404649a27AD35',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdb25f211ab05b1c97d595516f45794528a807ad8',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'eurtusd',
                'eurtusd',
                '0x9838eCcC42659FA8AA7daF2aD134b53984c9427B',
                '0x3b6831c0077a1e44ED0a21841C3bC4dC11bCE833',
                '0x4Fd86Ce7Ecea88F7E0aA78DC12625996Fb3a04bc',
                '0xc581b735a1688071a1746c968e0798d642ede491',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0xc581b735a1688071a1746c968e0798d642ede491',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x5D0F47B32fDd343BfA74cE221808e2abE4A53827',
                NULL
            ),
            (
                'Regular',
                'gusd',
                'gusd',
                '0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956',
                '0xD2967f45c4f384DEEa880F807Be904762a3DeA07',
                '0xC5cfaDA84E902aD92DD40194f0883ad49639b023',
                '0x056fd409e1d7a124bd7017459dfea2f387b6d5cd',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0x056fd409e1d7a124bd7017459dfea2f387b6d5cd',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x64448B78561690B70E17CBE8029a3e5c1bB7136e',
                NULL
            ),
            (
                'Regular',
                'hbtc',
                'hbtc',
                '0x4CA9b3063Ec5866A4B82E437059D2C43d1be596f',
                '0xb19059ebb43466C323583928285a49f558E572Fd',
                '0x4c18E409Dc8619bFb6a1cB56D114C3f592E0aE79',
                '0x0316eb71485b0ab14103307bf65a021042c6d380',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'husd',
                'husd',
                '0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604',
                '0x5B5CFE992AdAC0C9D48E05854B2d91C73a003858',
                '0x2db0E83599a91b508Ac268a6197b8B14F5e72840',
                '0xdf574c24545e5ffecb9a659c229253d4111d87e1',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0xdf574c24545e5ffecb9a659c229253d4111d87e1',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x09672362833d8f703D5395ef3252D4Bfa51c15cA',
                NULL
            ),
            (
                'Regular',
                'ib',
                'ib',
                '0x2dded6Da1BF5DBdF597C45fcFaa3194e53EcfeAf',
                '0x5282a4eF67D9C33135340fB3289cc1711c13638c',
                '0xF5194c3325202F456c95c1Cf0cA36f8475C1949f',
                '0x8e595470Ed749b85C6F7669de83EAe304C2ec68f',
                '0x76Eb2FE28b36B3ee97F3Adae0C69606eeDB2A37c',
                '0x48759F220ED983dB51fA7A8C0D2AAb8f3ce4166A',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'link',
                'link',
                '0xF178C0b5Bb7e7aBF4e12A4838C7b7c5bA2C623c0',
                '0xcee60cfa923170e4f8204ae08b4fa6a3f5656f3A',
                '0xFD4D8a17df4C27c1dD245d153ccf4499e806C87d',
                '0x514910771af9ca656af840dff83e8264ecf986cA',
                '0xbbc455cb4f1b9e4bfc4b73970d360c8f032efee6',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'linkusd',
                'linkusd',
                '0xE7a24EF0C5e95Ffb0f6684b813A78F2a3AD7D171',
                '0x6D65b498cb23deAba52db31c93Da9BFFb340FB8f',
                NULL,
                '0x0e2ec54fc0b509f445631bf4b91ab8168230c752',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0x0e2ec54fc0b509f445631bf4b91ab8168230c752',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x1de7f0866e2c4adAC7b457c58Cc25c8688CDa1f2',
                NULL
            ),
            (
                'Regular',
                'musd',
                'musd',
                '0x8474DdbE98F5aA3179B3B3F5942D724aFcdec9f6',
                '0x1AEf73d49Dedc4b1778d0706583995958Dc862e6',
                '0x5f626c30EC1215f4EdCc9982265E8b1F411D1352',
                '0xe2f2a5c287993345a840db3b0845fbc70f5935a5',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0xe2f2a5c287993345a840db3b0845fbc70f5935a5',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x803A2B40c5a9BB2B86DD630B274Fa2A9202874C2',
                NULL
            ),
            (
                'Regular',
                'obtc',
                'obtc',
                '0xd81dA8D904b52208541Bade1bD6595D8a251F8dd',
                '0x2fE94ea3d5d4a175184081439753DE15AeF9d614',
                '0x11137B10C210b579405c21A07489e28F3c040AB1',
                '0x8064d9ae6cdf087b1bcd5bdf3531bd5d8c537a68',
                '0x075b1bb99792c9e1041ba13afef80c91a1e70fb3',
                NULL,
                '0x8064d9ae6cdf087b1bcd5bdf3531bd5d8c537a68',
                '0xeb4c2781e4eba804ce9a9803c67d0893436bb27d',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                '0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6',
                '0xd5BCf53e2C81e1991570f33Fa881c49EEa570C8d',
                NULL
            ),
            (
                'Regular',
                'pax',
                'pax',
                '0x06364f10B501e868329afBc005b3492902d6C763',
                '0xD905e2eaeBe188fc92179b6350807D8bd91Db0D8',
                '0x64E3C23bfc40722d3B649844055F1D51c1ac041d',
                '0x99d1fa417f94dcd62bfe781a1213c092a47041bc',
                '0x9777d7E2b60bB01759D0E2f8be2095df444cb07e',
                '0x1bE5d71F2dA660BFdee8012dDc58D024448A0A59',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x8E870D67F660D95d5be530380D0eC0bd388289E1',
                '0xA50cCc70b6a011CffDdf45057E39679379187287',
                '0x8e870d67f660d95d5be530380d0ec0bd388289e1'
            ),
            (
                'Regular',
                'pbtc',
                'pbtc',
                '0x7F55DDe206dbAD629C080068923b36fe9D6bDBef',
                '0xDE5331AC4B3630f94853Ff322B66407e0D6331E8',
                '0xd7d147c6Bb90A718c3De8C0568F9B560C79fa416',
                '0x5228a22e72ccc52d415ecfd199f99d0665e7733B',
                '0x075b1bb99792c9e1041ba13afef80c91a1e70fb3',
                NULL,
                '0x5228a22e72ccc52d415ecfd199f99d0665e7733B',
                '0xeb4c2781e4eba804ce9a9803c67d0893436bb27d',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                '0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6',
                '0x11F419AdAbbFF8d595E7d5b223eee3863Bb3902c',
                NULL
            ),
            (
                'Regular',
                'rai',
                'rai',
                '0x618788357D0EBd8A37e763ADab3bc575D54c2C7d',
                '0x6BA5b4e438FA0aAf7C1bD179285aF65d13bD3D90',
                '0x66ec719045bBD62db5eBB11184c18237D3Cc2E62',
                '0x03ab458634910AaD20eF5f1C8ee96F1D6ac54919',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0x03ab458634910aad20ef5f1c8ee96f1d6ac54919',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0xcB636B81743Bb8a7F1E355DEBb7D33b07009cCCc',
                NULL
            ),
            (
                'Regular',
                'ren',
                'ren',
                '0x93054188d876f558f4a66B2EF1d97d16eDf0895B',
                '0x49849C98ae39Fff122806C06791Fa73784FB3675',
                '0xB1F2cdeC61db658F091671F5f199635aEF202CAc',
                '0xeb4c2781e4eba804ce9a9803c67d0893436bb27d',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'reth',
                'reth',
                '0xF9440930043eb3997fc70e1339dBb11F341de7A8',
                '0x53a901d48795C58f485cBB38df08FA96a24669D5',
                '0x824F13f1a2F29cFEEa81154b46C0fc820677A637',
                '0x9559Aaa82d9649C7A7b220E7c461d2E74c9a3593',
                '0x0000000000000000000000000000000000000000',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'rsv',
                'rsv',
                '0xC18cC39da8b11dA8c3541C598eE022258F9744dA',
                '0xC2Ee6b0334C261ED60C72f6054450b61B8f18E35',
                '0x4dC4A289a8E33600D8bD4cf5F6313E43a37adec7',
                '0x196f4727526ea7fb1e17b2071b3d8eaa38486988',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0x196f4727526ea7fb1e17b2071b3d8eaa38486988',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0xBE175115BF33E12348ff77CcfEE4726866A0Fbd5',
                NULL
            ),
            (
                'Regular',
                'saave',
                'saave',
                '0xEB16Ae0052ed37f479f7fe63849198Df1765a733',
                '0x02d341CcB60fAaf662bC0554d13778015d1b285c',
                '0x462253b8F74B72304c145DB0e4Eebd326B22ca39',
                '0x028171bca77440897b824ca71d1c56cac55b68a3',
                '0x6c5024cd4f8a59110119c56f8933403a539555eB',
                NULL,
                '0x6B175474E89094C44Da98b954EedeAC495271d0f',
                '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51',
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'sbtc',
                'sbtc',
                '0x7fC77b5c7614E1533320Ea6DDc2Eb61fa00A9714',
                '0x075b1bb99792c9E1041bA13afEf80C91a1e70fB3',
                '0x705350c4BcD35c9441419DdD5d2f097d7a55410f',
                '0xeb4c2781e4eba804ce9a9803c67d0893436bb27d',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                '0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'seth',
                'seth',
                '0xc5424b857f758e906013f3555dad202e4bdb4567',
                '0xA3D87FffcE63B53E0d54fAa1cc983B7eB0b74A9c',
                '0x3C0FFFF15EA30C35d7A85B85c0782D6c94e1d238',
                '0x0000000000000000000000000000000000000000',
                '0x5e74c9036fb86bd7ecdcb084a0673efc32ea31cB',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'spelleth',
                'spelleth',
                '0x98638FAcf9a3865cd033F36548713183f6996122',
                '0x8282BD15dcA2EA2bDf24163E8f2781B30C43A2ef',
                '0x08380a4999Be1a958E2abbA07968d703C7A3027c',
                '0x0000000000000000000000000000000000000000',
                '0x090185f2135308bad17527004364ebcc2d37e5f6',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'steth',
                'steth',
                '0xDC24316b9AE028F1497c275EB9192a3Ea0f67022',
                '0x06325440D014e39736583c165C2963BA99fAf14e',
                '0x182B723a58739a9c974cFDB385ceaDb237453c28',
                '0x0000000000000000000000000000000000000000',
                '0xae7ab96520de3a18e5e111b5eaab095312d7fe84',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'susdv2',
                'susd',
                '0xA5407eAE9Ba41422680e2e00537571bcC53efBfd', --pool_address
                '0xC25a3A3b969415c80451098fa907EC722572917f', --token_address
                '0xA90996896660DEcC6E997655E065b23788857849', --gauge_address
                '0x6b175474e89094c44da98b954eedeac495271d0f', --coin0
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', --coin1
                '0xdac17f958d2ee523a2206206994597c13d831ec7', --coin2
                '0x6b175474e89094c44da98b954eedeac495271d0f', --undercoin0
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', --undercoin1
                '0xdac17f958d2ee523a2206206994597c13d831ec7', --undercoin2
                '0x57Ab1E02fEE23774580C119740129eAC7081e9D3', --undercoin3
                '0xFCBa3E75865d2d561BE8D220616520c171F12851', --deposit_contract
                '0x57ab1ec28d129707052df4df418d58a2d46d5f51' --coin3
            ),
            (
                'Regular',
                'tbtc',
                'tbtc',
                '0xC25099792E9349C7DD09759744ea681C7de2cb66',
                '0x64eda51d3Ad40D56b9dFc5554E06F94e1Dd786Fd',
                '0x6828bcF74279eE32f2723eC536c22c51Eed383C6',
                '0x8daebade922df735c38c80c7ebd708af50815faA',
                '0x075b1bb99792c9e1041ba13afef80c91a1e70fb3',
                NULL,
                '0x8daebade922df735c38c80c7ebd708af50815faA',
                '0xeb4c2781e4eba804ce9a9803c67d0893436bb27d',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                '0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6',
                '0xaa82ca713D94bBA7A89CEAB55314F9EfFEdDc78c',
                NULL
            ),
            (
                'Regular',
                'teth',
                'teth',
                '0x752eBeb79963cf0732E9c0fec72a49FD1DEfAEAc',
                '0xCb08717451aaE9EF950a2524E33B6DCaBA60147B',
                '0x6070fBD4E608ee5391189E7205d70cc4A274c017',
                '0x0000000000000000000000000000000000000000',
                '0xcdf7028ceab81fa0c6971208e83fa7872994bee5',
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            ),
            (
                'Regular',
                'tricrypto',
                'tricrypto',
                '0x80466c64868E1ab14a1Ddf27A676C3fcBE638Fe5',
                '0xcA3d75aC011BF5aD07a98d02f18225F9bD9A6BDf',
                '0x6955a55416a06839309018A8B0cB72c4DDC11f15',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                NULL,
                NULL,
                NULL,
                NULL,
                '0x331aF2E331bd619DefAa5DAc6c038f53FCF9F785',
                NULL
            ),
            (
                'Regular',
                'tricrypto2',
                'tricrypto2',
                '0xD51a44d3FaE010294C616388b506AcdA1bfAAE46',
                '0xc4AD29ba4B3c580e6D59105FFf484999997675Ff',
                '0xDeFd8FdD20e0f34115C7018CCfb655796F6B2168',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                NULL,
                NULL,
                NULL,
                NULL,
                '0x3993d34e7e99Abf6B6f367309975d1360222D446',
                NULL
            ),
            (
                'Regular',
                'usdk',
                'usdk',
                '0x3E01dD8a5E1fb3481F0F589056b428Fc308AF0FB',
                '0x97E2768e8E73511cA874545DC5Ff8067eB19B787',
                '0xC2b1DF84112619D190193E48148000e3990Bf627',
                '0x1c48f86ae57291f7686349f12601910bd8d470bB',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0x1c48f86ae57291f7686349f12601910bd8d470bB',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0xF1f85a74AD6c64315F85af52d3d46bF715236ADc',
                NULL
            ),
            (
                'Regular',
                'usdn',
                'usdn',
                '0x0f9cb53Ebe405d49A0bbdBD291A65Ff571bC83e1',
                '0x4f3E8F405CF5aFC05D68142F3783bDfE13811522',
                '0xF98450B5602fa59CC66e1379DFfB6FDDc724CfC4',
                '0x674c6ad92fd080e4004b2312b45f796a192d27a0',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0x674c6ad92fd080e4004b2312b45f796a192d27a0',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x094d12e5b541784701FD8d65F11fc0598FBC6332',
                NULL
            ),
            (
                'Regular',
                'usdp',
                'usdp',
                '0x42d7025938bEc20B69cBae5A77421082407f053A',
                '0x7Eb40E450b9655f4B3cC4259BCC731c63ff55ae6',
                '0x055be5DDB7A925BfEF3417FC157f53CA77cA7222',
                '0x1456688345527be1f37e9e627da0837d6f08c925',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0x1456688345527be1f37e9e627da0837d6f08c925',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0x3c8cAee4E09296800f8D29A68Fa3837e2dae4940',
                NULL
            ),
            (
                'Regular',
                'usdt',
                'usdt',
                '0x52EA46506B9CC5Ef470C5bf89f17Dc28bB35D85c',
                '0x9fC689CCaDa600B6DF723D9E47D84d76664a1F23',
                '0xBC89cd85491d81C6AD2954E6d0362Ee29fCa8F53', --gauge_contract
                '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643', --coin0
                '0x39aa39c021dfbae8fac545936693ac917d5e7563', --coin1
                '0xdac17f958d2ee523a2206206994597c13d831ec7', --coin2
                '0x6b175474e89094c44da98b954eedeac495271d0f', --undercoin0
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', --undercoin1
                '0xdac17f958d2ee523a2206206994597c13d831ec7', --undercoin2
                NULL, --undercoin3
                '0xac795D2c97e60DF6a99ff1c814727302fD747a80', --deposit_contract
                NULL --coin3
            ),
            (
                'Regular',
                'ust',
                'ust',
                '0x890f4e345B1dAED0367A877a1612f86A1f86985f',
                '0x94e131324b6054c0D789b190b2dAC504e4361b53',
                '0x3B7020743Bc2A4ca9EaF9D0722d42E20d6935855',
                '0xa47c8bf37f92abed4a126bda807a7b7498661acd',
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490',
                NULL,
                '0xa47c8bf37f92abed4a126bda807a7b7498661acd',
                '0x6b175474e89094c44da98b954eedeac495271d0f',
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
                '0xdac17f958d2ee523a2206206994597c13d831ec7',
                '0xB0a0716841F2Fc03fbA72A891B8Bb13584F52F2d',
                NULL
            ),
            (
                'Regular',
                'xautusd',
                'xautusd',
                '0xAdCFcf9894335dC340f6Cd182aFA45999F45Fc44', --pool_address
                '0x8484673cA7BfF40F82B041916881aeA15ee84834', --token_address
                '0x1B3E14157ED33F60668f2103bCd5Db39a1573E5B', --gauge_contract
                '0x68749665ff8d2d112fa859aa293f07a622782f38', --coin0
                '0x6c3f90f043a72fa612cbac8115ee7e52bde6e490', --coin1
                NULL, --coin2
                '0x68749665ff8d2d112fa859aa293f07a622782f38', --undercoin0
                '0x6b175474e89094c44da98b954eedeac495271d0f', --undercoin1
                '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', --undercoin2
                '0xdac17f958d2ee523a2206206994597c13d831ec7', --undercoin3
                '0xc5FA220347375ac4f91f9E4A4AAb362F22801504', --deposit_contract
                NULL --coin3
            )

            , (
                'Regular' --version
                , 'fraxusdc' --name
                , 'FRAX/USDC' --symbol
                , '0xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' --pool_address
                , '0x3175df0976dfa876431c2e9ee6bc45b65d3473cc' --token_address
                , '0xcfc25170633581bf896cb6cdee170e3e3aa59503' --gauge_contract
                , '0x853d955aCEf822Db058eb8505911ED77F175b99e' --coin0
                , '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' --coin1
                , NULL --coin2
                , null --undercoin0
                , null --undercoin1
                , null --undercoin2
                , null --undercoin3
                , null --deposit_contract
                , null --coin3
            )

            , (
                'Regular' --version
                , 'sbtc2' --name
                , 'sbtc2' --symbol
                , '0xf253f83aca21aabd2a20553ae0bf7f65c755a07f' --pool_address
                , '0x051d7e5609917bd9b73f04bac0ded8dd46a74301' --token_address
                , '0x6d787113f23bed1d5e1530402b3f364d0a6e5af3' --gauge_contract
                , '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599' --coin0
                , '0xfE18be6b3Bd88A2D2A7f928d00292E7a9963CfC6' --coin1
                , NULL --coin2
                , null --undercoin0
                , null --undercoin1
                , null --undercoin2
                , null --undercoin3
                , null --deposit_contract
                , null --coin3
            )

    ) AS temp_table (
        version,
        `name`,
        symbol,
        pool_address,
        token_address,
        gauge_contract,
        coin0,
        coin1,
        coin2,
        undercoin0,
        undercoin1,
        undercoin2,
        undercoin3,
        deposit_contract,
        coin3
    )
