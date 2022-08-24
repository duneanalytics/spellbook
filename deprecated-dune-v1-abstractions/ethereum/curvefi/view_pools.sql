CREATE OR REPLACE VIEW curvefi.view_pools (
    version,
    name,
    symbol,
    pool_address,
    decoded,
    dune_namespace,
    dune_table_name,
    A,
    mid_fee,
    out_fee,
    token_address,
    deposit_contract,
    coin0,
    coin1,
    coin2,
    coin3,
    undercoin0,
    undercoin1,
    undercoin2,
    undercoin3,
    gauge_contract
) AS

WITH regular_poolDeployed AS (
    WITH regular_pools AS (
        WITH data_json AS (
            SELECT data_json::json
            FROM (values ('
                {"data":
                    [{
                        "version": "Regular",
                        "name": "3pool",
                        "symbol": "3pool",
                        "pool_address": "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7",
                        "token_address": "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
                        "gauge_contract": "0xbFcF63294aD7105dEa65aA58F8AE5BE2D9d0952A",
                        "coin0": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "coin1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "coin2": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "aave",
                        "symbol": "aave",
                        "pool_address": "0xDeBF20617708857ebe4F679508E7b7863a8A8EeE",
                        "token_address": "0xFd2a8fA60Abd58Efe3EeE34dd494cD491dC14900",
                        "gauge_contract": "0xd662908ADA2Ea1916B3318327A97eB18aD588b5d",
                        "coin0": "0x028171bca77440897b824ca71d1c56cac55b68a3",
                        "coin1": "0xbcca60bb61934080951369a648fb03df4f96263c",
                        "coin2": "0x3ed3b47dd13ec9a98b44e6204a523e766b225811",
                        "undercoin0": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin2": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "alusd",
                        "symbol": "alusd",
                        "pool_address": "0x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c",
                        "token_address": "0x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c",
                        "gauge_contract": "0x9582C4ADACB3BCE56Fea3e590F05c3ca2fb9C477",
                        "coin0": "0xbc6da0fe9ad5f3b0d58160288917aa56653660e9",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0xbc6da0fe9ad5f3b0d58160288917aa56653660e9",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "ankreth",
                        "symbol": "ankreth",
                        "pool_address": "0xA96A65c051bF88B4095Ee1f2451C2A9d43F53Ae2",
                        "token_address": "0xaA17A236F2bAdc98DDc0Cf999AbB47D47Fc0A6Cf",
                        "gauge_contract": "0x6d10ed2cF043E6fcf51A0e7b4C2Af3Fa06695707",
                        "coin0": "0x0000000000000000000000000000000000000000",
                        "coin1": "0xe95a203b1a91a908f9b9ce46459d101078c2c3cb"
                    },
                    {
                        "version": "Regular",
                        "name": "bbtc",
                        "symbol": "bbtc",
                        "pool_address": "0x071c661B4DeefB59E2a3DdB20Db036821eeE8F4b",
                        "token_address": "0x410e3E86ef427e30B9235497143881f717d93c2A",
                        "deposit_contract": "0xC45b2EEe6e09cA176Ca3bB5f7eEe7C47bF93c756",
                        "gauge_contract": "0xdFc7AdFa664b08767b735dE28f9E84cd30492aeE",
                        "coin0": "0x9be89d2a4cd102d8fecc6bf9da793be995c22541",
                        "coin1": "0x075b1bb99792c9e1041ba13afef80c91a1e70fb3",
                        "undercoin0": "0x9be89d2a4cd102d8fecc6bf9da793be995c22541",
                        "undercoin1": "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d",
                        "undercoin2": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                        "undercoin3": "0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6"
                    },
                    {
                        "version": "Regular",
                        "name": "busd",
                        "symbol": "busd",
                        "pool_address": "0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27",
                        "token_address": "0x3B3Ac5386837Dc563660FB6a0937DFAa5924333B",
                        "deposit_contract": "0xb6c057591E073249F2D9D88Ba59a46CFC9B59EdB",
                        "gauge_contract": "0x69Fb7c45726cfE2baDeE8317005d3F94bE838840",
                        "coin0": "0xc2cb1040220768554cf699b0d863a3cd4324ce32",
                        "coin1": "0x26ea744e5b887e5205727f55dfbe8685e3b21951",
                        "coin2": "0xe6354ed5bc4b393a5aad09f21c46e101e692d447",
                        "coin3": "0x04bc0ab673d88ae9dbc9da2380cb6b79c4bca9ae",
                        "undercoin0": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin2": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                        "undercoin3": "0x4fabb145d64652a948d72533023f6e7a623c7c53"
                    },
                    {
                        "version": "Regular",
                        "name": "busdv2",
                        "symbol": "busdv2",
                        "pool_address": "0x4807862AA8b2bF68830e4C8dc86D0e9A998e085a",
                        "token_address": "0x4807862AA8b2bF68830e4C8dc86D0e9A998e085a",
                        "gauge_contract": "0xd4B22fEdcA85E684919955061fDf353b9d38389b",
                        "coin0": "0x4fabb145d64652a948d72533023f6e7a623c7c53",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x4fabb145d64652a948d72533023f6e7a623c7c53",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "compound",
                        "symbol": "compound",
                        "pool_address": "0xA2B47E3D5c44877cca798226B7B8118F9BFb7A56",
                        "token_address": "0x845838DF265Dcd2c412A1Dc9e959c7d08537f8a2",
                        "deposit_contract": "0xeB21209ae4C2c9FF2a86ACA31E123764A3B6Bc06",
                        "gauge_contract": "0x7ca5b0a2910B33e9759DC7dDB0413949071D7575",
                        "coin0": "0x5d3a536e4d6dbd6114cc1ead35777bab948e3643",
                        "coin1": "0x39aa39c021dfbae8fac545936693ac917d5e7563",
                        "undercoin0": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
                    },
                    {
                        "version": "Regular",
                        "name": "crveth",
                        "symbol": "crveth",
                        "pool_address": "0x8301AE4fc9c624d1D396cbDAa1ed877821D7C511",
                        "token_address": "0xEd4064f376cB8d68F770FB1Ff088a3d0F3FF5c4d",
                        "gauge_contract": "0x1cEBdB0856dd985fAe9b8fEa2262469360B8a3a6",
                        "coin0": "0x0000000000000000000000000000000000000000",
                        "coin1": "0xd533a949740bb3306d119cc777fa900ba034cd52"
                    },
                    {
                        "version": "Regular",
                        "name": "cvxeth",
                        "symbol": "cvxeth",
                        "pool_address": "0xB576491F1E6e5E62f1d8F26062Ee822B40B0E0d4",
                        "token_address": "0x3A283D9c08E8b55966afb64C515f5143cf907611",
                        "gauge_contract": "0x7E1444BA99dcdFfE8fBdb42C02F0005D14f13BE1",
                        "coin0": "0x0000000000000000000000000000000000000000",
                        "coin1": "0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b"
                    },
                    {
                        "version": "Regular",
                        "name": "dusd",
                        "symbol": "dusd",
                        "pool_address": "0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c",
                        "token_address": "0x3a664Ab939FD8482048609f652f9a0B0677337B9",
                        "deposit_contract": "0x61E10659fe3aa93d036d099405224E4Ac24996d0",
                        "gauge_contract": "0xAEA6c312f4b3E04D752946d329693F7293bC2e6D",
                        "coin0": "0x5bc25f649fc4e26069ddf4cf4010f9f706c23831",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x5bc25f649fc4e26069ddf4cf4010f9f706c23831",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "eurs",
                        "symbol": "eurs",
                        "pool_address": "0x0Ce6a5fF5217e38315f87032CF90686C96627CAA",
                        "token_address": "0x194eBd173F6cDacE046C53eACcE9B953F28411d1",
                        "gauge_contract": "0x90Bb609649E0451E5aD952683D64BD2d1f245840",
                        "coin0": "0xdb25f211ab05b1c97d595516f45794528a807ad8",
                        "coin1": "0xd71ecff9342a5ced620049e616c5035f1db98620"
                    },
                    {
                        "version": "Regular",
                        "name": "eursusd",
                        "symbol": "eursusd",
                        "pool_address": "0x98a7F18d4E56Cfe84E3D081B40001B3d5bD3eB8B",
                        "token_address": "0x3d229e1b4faab62f621ef2f6a610961f7bd7b23b",
                        "gauge_contract": "0x65CA7Dc5CB661fC58De57B1E1aF404649a27AD35",
                        "coin0": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "coin1": "0xdb25f211ab05b1c97d595516f45794528a807ad8"
                    },
                    {
                        "version": "Regular",
                        "name": "eurt",
                        "symbol": "eurt",
                        "pool_address": "0xfd5db7463a3ab53fd211b4af195c5bccc1a03890",
                        "token_address": "0xfd5db7463a3ab53fd211b4af195c5bccc1a03890",
                        "gauge_contract": "0xe8060Ad8971450E624d5289A10017dD30F5dA85F",
                        "coin0": "0xc581b735a1688071a1746c968e0798d642ede491",
                        "coin1": "0xd71ecff9342a5ced620049e616c5035f1db98620"
                    },
                    {
                        "version": "Regular",
                        "name": "eurtusd",
                        "symbol": "eurtusd",
                        "pool_address": "0x9838eCcC42659FA8AA7daF2aD134b53984c9427b",
                        "token_address": "0x3b6831c0077a1e44ED0a21841C3bC4dC11bCE833",
                        "deposit_contract": "0x5D0F47B32fDd343BfA74cE221808e2abE4A53827",
                        "gauge_contract": "0x4Fd86Ce7Ecea88F7E0aA78DC12625996Fb3a04bC",
                        "coin0": "0xc581b735a1688071a1746c968e0798d642ede491",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0xc581b735a1688071a1746c968e0798d642ede491",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "frax",
                        "symbol": "frax",
                        "pool_address": "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B",
                        "token_address": "0xd632f22692FaC7611d2AA1C0D552930D43CAEd3B",
                        "gauge_contract": "0x72E158d38dbd50A483501c24f792bDAAA3e7D55C",
                        "coin0": "0x853d955acef822db058eb8505911ed77f175b99e",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x853d955acef822db058eb8505911ed77f175b99e",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "gusd",
                        "symbol": "gusd",
                        "pool_address": "0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956",
                        "token_address": "0xD2967f45c4f384DEEa880F807Be904762a3DeA07",
                        "deposit_contract": "0x64448B78561690B70E17CBE8029a3e5c1bB7136e",
                        "gauge_contract": "0xC5cfaDA84E902aD92DD40194f0883ad49639b023",
                        "coin0": "0x056fd409e1d7a124bd7017459dfea2f387b6d5cd",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x056fd409e1d7a124bd7017459dfea2f387b6d5cd",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "hbtc",
                        "symbol": "hbtc",
                        "pool_address": "0x4CA9b3063Ec5866A4B82E437059D2C43d1be596F",
                        "token_address": "0xb19059ebb43466C323583928285a49f558E572Fd",
                        "gauge_contract": "0x4c18E409Dc8619bFb6a1cB56D114C3f592E0aE79",
                        "coin0": "0x0316eb71485b0ab14103307bf65a021042c6d380",
                        "coin1": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
                    },
                    {
                        "version": "Regular",
                        "name": "husd",
                        "symbol": "husd",
                        "pool_address": "0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604",
                        "token_address": "0x5B5CFE992AdAC0C9D48E05854B2d91C73a003858",
                        "deposit_contract": "0x09672362833d8f703D5395ef3252D4Bfa51c15ca",
                        "gauge_contract": "0x2db0E83599a91b508Ac268a6197b8B14F5e72840",
                        "coin0": "0xdf574c24545e5ffecb9a659c229253d4111d87e1",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0xdf574c24545e5ffecb9a659c229253d4111d87e1",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "ib",
                        "symbol": "ib",
                        "pool_address": "0x2dded6Da1BF5DBdF597C45fcFaa3194e53EcfeAF",
                        "token_address": "0x5282a4eF67D9C33135340fB3289cc1711c13638C",
                        "gauge_contract": "0xF5194c3325202F456c95c1Cf0cA36f8475C1949F",
                        "coin0": "0x8e595470Ed749b85C6F7669de83EAe304C2ec68F",
                        "coin1": "0x76Eb2FE28b36B3ee97F3Adae0C69606eeDB2A37c",
                        "coin2": "0x48759F220ED983dB51fA7A8C0D2AAb8f3ce4166a",
                        "undercoin0": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin2": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "link",
                        "symbol": "link",
                        "pool_address": "0xF178C0b5Bb7e7aBF4e12A4838C7b7c5bA2C623c0",
                        "token_address": "0xcee60cfa923170e4f8204ae08b4fa6a3f5656f3a",
                        "gauge_contract": "0xFD4D8a17df4C27c1dD245d153ccf4499e806C87D",
                        "coin0": "0x514910771af9ca656af840dff83e8264ecf986ca",
                        "coin1": "0xbbc455cb4f1b9e4bfc4b73970d360c8f032efee6"
                    },
                    {
                        "version": "Regular",
                        "name": "linkusd",
                        "symbol": "linkusd",
                        "pool_address": "0xE7a24EF0C5e95Ffb0f6684b813A78F2a3AD7D171",
                        "token_address": "0x6D65b498cb23deAba52db31c93Da9BFFb340FB8F",
                        "deposit_contract": "0x1de7f0866e2c4adAC7b457c58Cc25c8688CDa1f2",
                        "coin0": "0x0e2ec54fc0b509f445631bf4b91ab8168230c752",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x0e2ec54fc0b509f445631bf4b91ab8168230c752",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "lusd",
                        "symbol": "lusd",
                        "pool_address": "0xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA",
                        "token_address": "0xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA",
                        "gauge_contract": "0x9B8519A9a00100720CCdC8a120fBeD319cA47a14",
                        "coin0": "0x5f98805a4e8be255a32880fdec7f6728c6568ba0",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x5f98805a4e8be255a32880fdec7f6728c6568ba0",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "mim",
                        "symbol": "mim",
                        "pool_address": "0x5a6A4D54456819380173272A5E8E9B9904BdF41B",
                        "token_address": "0x5a6A4D54456819380173272A5E8E9B9904BdF41B",
                        "gauge_contract": "0xd8b712d29381748dB89c36BCa0138d7c75866ddF",
                        "coin0": "0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "musd",
                        "symbol": "musd",
                        "pool_address": "0x8474DdbE98F5aA3179B3B3F5942D724aFcdec9f6",
                        "token_address": "0x1AEf73d49Dedc4b1778d0706583995958Dc862e6",
                        "deposit_contract": "0x803A2B40c5a9BB2B86DD630B274Fa2A9202874C2",
                        "gauge_contract": "0x5f626c30EC1215f4EdCc9982265E8b1F411D1352",
                        "coin0": "0xe2f2a5c287993345a840db3b0845fbc70f5935a5",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0xe2f2a5c287993345a840db3b0845fbc70f5935a5",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "obtc",
                        "symbol": "obtc",
                        "pool_address": "0xd81dA8D904b52208541Bade1bD6595D8a251F8dd",
                        "token_address": "0x2fE94ea3d5d4a175184081439753DE15AeF9d614",
                        "deposit_contract": "0xd5BCf53e2C81e1991570f33Fa881c49EEa570C8D",
                        "gauge_contract": "0x11137B10C210b579405c21A07489e28F3c040AB1",
                        "coin0": "0x8064d9ae6cdf087b1bcd5bdf3531bd5d8c537a68",
                        "coin1": "0x075b1bb99792c9e1041ba13afef80c91a1e70fb3",
                        "undercoin0": "0x8064d9ae6cdf087b1bcd5bdf3531bd5d8c537a68",
                        "undercoin1": "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d",
                        "undercoin2": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                        "undercoin3": "0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6"
                    },
                    {
                        "version": "Regular",
                        "name": "pax",
                        "symbol": "pax",
                        "pool_address": "0x06364f10B501e868329afBc005b3492902d6C763",
                        "token_address": "0xD905e2eaeBe188fc92179b6350807D8bd91Db0D8",
                        "deposit_contract": "0xA50cCc70b6a011CffDdf45057E39679379187287",
                        "gauge_contract": "0x64E3C23bfc40722d3B649844055F1D51c1ac041d",
                        "coin0": "0x99d1fa417f94dcd62bfe781a1213c092a47041bc",
                        "coin1": "0x9777d7E2b60bB01759D0E2f8be2095df444cb07E",
                        "coin2": "0x1bE5d71F2dA660BFdee8012dDc58D024448A0A59",
                        "coin3": "0x8e870d67f660d95d5be530380d0ec0bd388289e1",
                        "undercoin0": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin2": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                        "undercoin3": "0x8E870D67F660D95d5be530380D0eC0bd388289E1"
                    },
                    {
                        "version": "Regular",
                        "name": "pbtc",
                        "symbol": "pbtc",
                        "pool_address": "0x7F55DDe206dbAD629C080068923b36fe9D6bDBeF",
                        "token_address": "0xDE5331AC4B3630f94853Ff322B66407e0D6331E8",
                        "deposit_contract": "0x11F419AdAbbFF8d595E7d5b223eee3863Bb3902C",
                        "gauge_contract": "0xd7d147c6Bb90A718c3De8C0568F9B560C79fa416",
                        "coin0": "0x5228a22e72ccc52d415ecfd199f99d0665e7733b",
                        "coin1": "0x075b1bb99792c9e1041ba13afef80c91a1e70fb3",
                        "undercoin0": "0x5228a22e72ccc52d415ecfd199f99d0665e7733b",
                        "undercoin1": "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d",
                        "undercoin2": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                        "undercoin3": "0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6"
                    },
                    {
                        "version": "Regular",
                        "name": "rai",
                        "symbol": "rai",
                        "pool_address": "0x618788357D0EBd8A37e763ADab3bc575D54c2C7d",
                        "token_address": "0x6BA5b4e438FA0aAf7C1bD179285aF65d13bD3D90",
                        "deposit_contract": "0xcB636B81743Bb8a7F1E355DEBb7D33b07009cCCC",
                        "gauge_contract": "0x66ec719045bBD62db5eBB11184c18237D3Cc2E62",
                        "coin0": "0x03ab458634910AaD20eF5f1C8ee96F1D6ac54919",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x03ab458634910aad20ef5f1c8ee96f1d6ac54919",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "ren",
                        "symbol": "ren",
                        "pool_address": "0x93054188d876f558f4a66B2EF1d97d16eDf0895B",
                        "token_address": "0x49849C98ae39Fff122806C06791Fa73784FB3675",
                        "gauge_contract": "0xB1F2cdeC61db658F091671F5f199635aEF202CAC",
                        "coin0": "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d",
                        "coin1": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
                    },
                    {
                        "version": "Regular",
                        "name": "reth",
                        "symbol": "reth",
                        "pool_address": "0xF9440930043eb3997fc70e1339dBb11F341de7A8",
                        "token_address": "0x53a901d48795C58f485cBB38df08FA96a24669D5",
                        "gauge_contract": "0x824F13f1a2F29cFEEa81154b46C0fc820677A637",
                        "coin0": "0x9559Aaa82d9649C7A7b220E7c461d2E74c9a3593",
                        "coin1": "0x0000000000000000000000000000000000000000"
                    },
                    {
                        "version": "Regular",
                        "name": "rsv",
                        "symbol": "rsv",
                        "pool_address": "0xC18cC39da8b11dA8c3541C598eE022258F9744da",
                        "token_address": "0xC2Ee6b0334C261ED60C72f6054450b61B8f18E35",
                        "deposit_contract": "0xBE175115BF33E12348ff77CcfEE4726866A0Fbd5",
                        "gauge_contract": "0x4dC4A289a8E33600D8bD4cf5F6313E43a37adec7",
                        "coin0": "0x196f4727526ea7fb1e17b2071b3d8eaa38486988",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x196f4727526ea7fb1e17b2071b3d8eaa38486988",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "saave",
                        "symbol": "saave",
                        "pool_address": "0xEB16Ae0052ed37f479f7fe63849198Df1765a733",
                        "token_address": "0x02d341CcB60fAaf662bC0554d13778015d1b285C",
                        "gauge_contract": "0x462253b8F74B72304c145DB0e4Eebd326B22ca39",
                        "coin0": "0x028171bca77440897b824ca71d1c56cac55b68a3",
                        "coin1": "0x6c5024cd4f8a59110119c56f8933403a539555eb",
                        "undercoin0": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                        "undercoin1": "0x57Ab1ec28D129707052df4dF418D58a2D46d5f51"
                    },
                    {
                        "version": "Regular",
                        "name": "sbtc",
                        "symbol": "sbtc",
                        "pool_address": "0x7fC77b5c7614E1533320Ea6DDc2Eb61fa00A9714",
                        "token_address": "0x075b1bb99792c9E1041bA13afEf80C91a1e70fB3",
                        "gauge_contract": "0x705350c4BcD35c9441419DdD5d2f097d7a55410F",
                        "coin0": "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d",
                        "coin1": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                        "coin2": "0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6"
                    },
                    {
                        "version": "Regular",
                        "name": "seth",
                        "symbol": "seth",
                        "pool_address": "0xc5424b857f758e906013f3555dad202e4bdb4567",
                        "token_address": "0xA3D87FffcE63B53E0d54fAa1cc983B7eB0b74A9c",
                        "gauge_contract": "0x3C0FFFF15EA30C35d7A85B85c0782D6c94e1d238",
                        "coin0": "0x0000000000000000000000000000000000000000",
                        "coin1": "0x5e74c9036fb86bd7ecdcb084a0673efc32ea31cb"
                    },
                    {
                        "version": "Regular",
                        "name": "spelleth",
                        "symbol": "spelleth",
                        "pool_address": "0x98638FAcf9a3865cd033F36548713183f6996122",
                        "token_address": "0x8282BD15dcA2EA2bDf24163E8f2781B30C43A2ef",
                        "gauge_contract": "0x08380a4999Be1a958E2abbA07968d703C7A3027C",
                        "coin0": "0x0000000000000000000000000000000000000000",
                        "coin1": "0x090185f2135308bad17527004364ebcc2d37e5f6"
                    },
                    {
                        "version": "Regular",
                        "name": "steth",
                        "symbol": "steth",
                        "pool_address": "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022",
                        "token_address": "0x06325440D014e39736583c165C2963BA99fAf14E",
                        "gauge_contract": "0x182B723a58739a9c974cFDB385ceaDb237453c28",
                        "coin0": "0x0000000000000000000000000000000000000000",
                        "coin1": "0xae7ab96520de3a18e5e111b5eaab095312d7fe84"
                    },
                    {
                        "version": "Regular",
                        "name": "susdv2",
                        "symbol": "susd",
                        "pool_address": "0xA5407eAE9Ba41422680e2e00537571bcC53efBfD",
                        "token_address": "0xC25a3A3b969415c80451098fa907EC722572917F",
                        "deposit_contract": "0xFCBa3E75865d2d561BE8D220616520c171F12851",
                        "gauge_contract": "0xA90996896660DEcC6E997655E065b23788857849",
                        "coin0": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "coin1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "coin2": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                        "coin3": "0x57ab1ec28d129707052df4df418d58a2d46d5f51",
                        "undercoin0": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin2": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                        "undercoin3": "0x57Ab1E02fEE23774580C119740129eAC7081e9D3"
                    },
                    {
                        "version": "Regular",
                        "name": "tbtc",
                        "symbol": "tbtc",
                        "pool_address": "0xC25099792E9349C7DD09759744ea681C7de2cb66",
                        "token_address": "0x64eda51d3Ad40D56b9dFc5554E06F94e1Dd786Fd",
                        "deposit_contract": "0xaa82ca713D94bBA7A89CEAB55314F9EfFEdDc78c",
                        "gauge_contract": "0x6828bcF74279eE32f2723eC536c22c51Eed383C6",
                        "coin0": "0x8daebade922df735c38c80c7ebd708af50815faa",
                        "coin1": "0x075b1bb99792c9e1041ba13afef80c91a1e70fb3",
                        "undercoin0": "0x8daebade922df735c38c80c7ebd708af50815faa",
                        "undercoin1": "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d",
                        "undercoin2": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                        "undercoin3": "0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6"
                    },
                    {
                        "version": "Regular",
                        "name": "teth",
                        "symbol": "teth",
                        "pool_address": "0x752eBeb79963cf0732E9c0fec72a49FD1DEfAEAC",
                        "token_address": "0xCb08717451aaE9EF950a2524E33B6DCaBA60147B",
                        "gauge_contract": "0x6070fBD4E608ee5391189E7205d70cc4A274c017",
                        "coin0": "0x0000000000000000000000000000000000000000",
                        "coin1": "0xcdf7028ceab81fa0c6971208e83fa7872994bee5"
                    },
                    {
                        "version": "Regular",
                        "name": "tricrypto",
                        "symbol": "tricrypto",
                        "pool_address": "0x80466c64868E1ab14a1Ddf27A676C3fcBE638Fe5",
                        "token_address": "0xcA3d75aC011BF5aD07a98d02f18225F9bD9A6BDF",
                        "deposit_contract": "0x331aF2E331bd619DefAa5DAc6c038f53FCF9F785",
                        "gauge_contract": "0x6955a55416a06839309018A8B0cB72c4DDC11f15",
                        "coin0": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                        "coin1": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                        "coin2": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                    },
                    {
                        "version": "Regular",
                        "name": "tricrypto2",
                        "symbol": "tricrypto2",
                        "pool_address": "0xD51a44d3FaE010294C616388b506AcdA1bfAAE46",
                        "token_address": "0xc4AD29ba4B3c580e6D59105FFf484999997675Ff",
                        "deposit_contract": "0x3993d34e7e99Abf6B6f367309975d1360222D446",
                        "gauge_contract": "0xDeFd8FdD20e0f34115C7018CCfb655796F6B2168",
                        "coin0": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                        "coin1": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                        "coin2": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                    },
                    {
                        "version": "Regular",
                        "name": "tusd",
                        "symbol": "tusd",
                        "pool_address": "0xecd5e75afb02efa118af914515d6521aabd189f1",
                        "token_address": "0xecd5e75afb02efa118af914515d6521aabd189f1",
                        "deposit_contract": "0xA79828DF1850E8a3A3064576f380D90aECDD3359",
                        "gauge_contract": "0x359FD5d6417aE3D8D6497d9B2e7A890798262BA4",
                        "coin0": "0x0000000000085d4780b73119b644ae5ecd22b376",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x0000000000085d4780b73119b644ae5ecd22b376",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "usdk",
                        "symbol": "usdk",
                        "pool_address": "0x3E01dD8a5E1fb3481F0F589056b428Fc308AF0Fb",
                        "token_address": "0x97E2768e8E73511cA874545DC5Ff8067eB19B787",
                        "deposit_contract": "0xF1f85a74AD6c64315F85af52d3d46bF715236ADc",
                        "gauge_contract": "0xC2b1DF84112619D190193E48148000e3990Bf627",
                        "coin0": "0x1c48f86ae57291f7686349f12601910bd8d470bb",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x1c48f86ae57291f7686349f12601910bd8d470bb",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "usdn",
                        "symbol": "usdn",
                        "pool_address": "0x0f9cb53Ebe405d49A0bbdBD291A65Ff571bC83e1",
                        "token_address": "0x4f3E8F405CF5aFC05D68142F3783bDfE13811522",
                        "deposit_contract": "0x094d12e5b541784701FD8d65F11fc0598FBC6332",
                        "gauge_contract": "0xF98450B5602fa59CC66e1379DFfB6FDDc724CfC4",
                        "coin0": "0x674c6ad92fd080e4004b2312b45f796a192d27a0",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x674c6ad92fd080e4004b2312b45f796a192d27a0",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "usdp",
                        "symbol": "usdp",
                        "pool_address": "0x42d7025938bEc20B69cBae5A77421082407f053A",
                        "token_address": "0x7Eb40E450b9655f4B3cC4259BCC731c63ff55ae6",
                        "deposit_contract": "0x3c8cAee4E09296800f8D29A68Fa3837e2dae4940",
                        "gauge_contract": "0x055be5DDB7A925BfEF3417FC157f53CA77cA7222",
                        "coin0": "0x1456688345527be1f37e9e627da0837d6f08c925",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x1456688345527be1f37e9e627da0837d6f08c925",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "usdt",
                        "symbol": "usdt",
                        "pool_address": "0x52EA46506B9CC5Ef470C5bf89f17Dc28bB35D85C",
                        "token_address": "0x9fC689CCaDa600B6DF723D9E47D84d76664a1F23",
                        "deposit_contract": "0xac795D2c97e60DF6a99ff1c814727302fD747a80",
                        "gauge_contract": "0xBC89cd85491d81C6AD2954E6d0362Ee29fCa8F53",
                        "coin0": "0x5d3a536e4d6dbd6114cc1ead35777bab948e3643",
                        "coin1": "0x39aa39c021dfbae8fac545936693ac917d5e7563",
                        "coin2": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                        "undercoin0": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin2": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "ust",
                        "symbol": "ust",
                        "pool_address": "0x890f4e345B1dAED0367A877a1612f86A1f86985f",
                        "token_address": "0x94e131324b6054c0D789b190b2dAC504e4361b53",
                        "deposit_contract": "0xB0a0716841F2Fc03fbA72A891B8Bb13584F52F2d",
                        "gauge_contract": "0x3B7020743Bc2A4ca9EaF9D0722d42E20d6935855",
                        "coin0": "0xa47c8bf37f92abed4a126bda807a7b7498661acd",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0xa47c8bf37f92abed4a126bda807a7b7498661acd",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "xautusd",
                        "symbol": "xautusd",
                        "pool_address": "0xAdCFcf9894335dC340f6Cd182aFA45999F45Fc44",
                        "token_address": "0x8484673cA7BfF40F82B041916881aeA15ee84834",
                        "deposit_contract": "0xc5FA220347375ac4f91f9E4A4AAb362F22801504",
                        "gauge_contract": "0x1B3E14157ED33F60668f2103bCd5Db39a1573E5B",
                        "coin0": "0x68749665ff8d2d112fa859aa293f07a622782f38",
                        "coin1": "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490",
                        "undercoin0": "0x68749665ff8d2d112fa859aa293f07a622782f38",
                        "undercoin1": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin2": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin3": "0xdac17f958d2ee523a2206206994597c13d831ec7"
                    },
                    {
                        "version": "Regular",
                        "name": "y",
                        "symbol": "y",
                        "pool_address": "0x45F783CCE6B7FF23B2ab2D70e416cdb7D6055f51",
                        "token_address": "0xdF5e0e81Dff6FAF3A7e52BA697820c5e32D806A8",
                        "deposit_contract": "0xbBC81d23Ea2c3ec7e56D39296F0cbB648873a5d3",
                        "gauge_contract": "0xFA712EE4788C042e2B7BB55E6cb8ec569C4530c1",
                        "coin0": "0x16de59092dae5ccf4a1e6439d611fd0653f0bd01",
                        "coin1": "0xd6ad7a6750a7593e092a9b218d66c0a814a3436e",
                        "coin2": "0x83f798e925bcd4017eb265844fddabb448f1707d",
                        "coin3": "0x73a052500105205d34daf004eab301916da8190f",
                        "undercoin0": "0x6b175474e89094c44da98b954eedeac495271d0f",
                        "undercoin1": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "undercoin2": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                        "undercoin3": "0x0000000000085d4780B73119b644AE5ecd22b376"
                    }]
                }
            ') ) json (data_json)
        ),

        records AS (
            SELECT json_array_elements(data_json->'data') AS record
            FROM data_json
        )

        SELECT
            (record->>'version') AS version,
            (record->>'name') AS name,
            (record->>'symbol') AS symbol,
            REPLACE((record->>'pool_address'),'0x','\x')::bytea AS pool_address,
            REPLACE((record->>'token_address'),'0x','\x')::bytea AS token_address,
            REPLACE((record->>'deposit_contract'),'0x','\x')::bytea AS deposit_contract,
            REPLACE((record->>'gauge_contract'),'0x','\x')::bytea AS gauge_contract,
            REPLACE((record->>'coin0'),'0x','\x')::bytea AS coin0,
            REPLACE((record->>'coin1'),'0x','\x')::bytea AS coin1,
            REPLACE((record->>'coin2'),'0x','\x')::bytea AS coin2,
            REPLACE((record->>'coin3'),'0x','\x')::bytea AS coin3,
            REPLACE((record->>'undercoin0'),'0x','\x')::bytea AS undercoin0,
            REPLACE((record->>'undercoin1'),'0x','\x')::bytea AS undercoin1,
            REPLACE((record->>'undercoin2'),'0x','\x')::bytea AS undercoin2,
            REPLACE((record->>'undercoin3'),'0x','\x')::bytea AS undercoin3
        FROM records
    )
    SELECT
        version,
        name,
        symbol,
        pool_address,
        NULL::numeric AS A,
        NULL::numeric AS mid_fee,
        NULL::numeric AS out_fee,
        token_address,
        deposit_contract,
        coin0,
        coin1,
        coin2,
        coin3,
        undercoin0,
        undercoin1,
        undercoin2,
        undercoin3,
        gauge_contract
    FROM regular_pools
),

V1_poolDeployed AS (
    WITH plain_poolDeployed AS (
        WITH plain_calls AS (
            SELECT _name, _symbol, output_0, _coins, "_A", _fee
            FROM curvefi."CurveFactory_call_deploy_plain_pool1"
            WHERE call_success
            UNION ALL
            SELECT _name, _symbol, output_0, _coins, "_A", _fee
            FROM curvefi."CurveFactory_call_deploy_plain_pool0"
            WHERE call_success
            UNION ALL
            SELECT _name, _symbol, output_0, _coins, "_A", _fee
            FROM curvefi."CurveFactory_call_deploy_plain_pool"
        )

        SELECT
            'Factory V1 Plain' AS version,
            _name AS name,
            _symbol AS symbol,
            output_0 AS pool_address,
            "_A" AS A,
            _fee AS mid_fee,
            _fee AS out_fee,
            output_0 AS token_address,
            output_0 AS deposit_contract,
            _coins[1] AS coin0,
            _coins[2] AS coin1,
            _coins[3] AS coin2,
            _coins[4] AS coin3,
            NULL::bytea AS undercoin0,
            NULL::bytea AS undercoin1,
            NULL::bytea AS undercoin2,
            NULL::bytea AS undercoin3
            -- c.coins::text AS coins
        FROM plain_calls
    ),

    meta_poolDeployed AS (
        WITH meta_calls AS (
            SELECT _name, _symbol, output_0, call_tx_hash, _base_pool, _coin, "_A", _fee
            FROM curvefi."CurveFactory_call_deploy_metapool"
            WHERE call_success
            UNION ALL
            SELECT _name, _symbol, output_0, call_tx_hash, _base_pool, _coin, "_A", _fee
            FROM curvefi."CurveFactory_call_deploy_metapool0"
            WHERE call_success
        )

        SELECT
            'Factory V1 Meta' AS version,
            _name AS name,
            _symbol AS symbol,
            output_0 AS pool_address,
            "_A" AS A,
            _fee AS mid_fee,
            _fee AS out_fee,
            output_0 AS token_address,
            output_0 AS deposit_contract,
            _coin AS coin0,
            CASE
                WHEN _base_pool = '\xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7' THEN '\x6c3F90f043a72FA612cbac8115EE7e52BDe6E490'::bytea --changing from swap to token contract
                WHEN _base_pool = '\x7fc77b5c7614e1533320ea6ddc2eb61fa00a9714' THEN '\x075b1bb99792c9E1041bA13afEf80C91a1e70fB3'::bytea --changing from swap to token contract
                WHEN _base_pool = '\x93054188d876f558f4a66b2ef1d97d16edf0895b' THEN '\x49849C98ae39Fff122806C06791Fa73784FB3675'::bytea --changing from swap to token contract
                END AS coin1,
            NULL::bytea AS coin2,
            NULL::bytea AS coin3,
            _coin AS undercoin0, --Listing underlying coins for the ExchangeUnderlying function
            CASE
                WHEN _base_pool = '\xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7' THEN '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea
                WHEN _base_pool = '\x7fc77b5c7614e1533320ea6ddc2eb61fa00a9714' THEN '\xeb4c2781e4eba804ce9a9803c67d0893436bb27d'::bytea
                WHEN _base_pool = '\x93054188d876f558f4a66b2ef1d97d16edf0895b' THEN '\xeb4c2781e4eba804ce9a9803c67d0893436bb27d'::bytea
                END AS undercoin1,
            CASE
                WHEN _base_pool = '\xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7' THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea
                WHEN _base_pool = '\x7fc77b5c7614e1533320ea6ddc2eb61fa00a9714' THEN '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea
                WHEN _base_pool = '\x93054188d876f558f4a66b2ef1d97d16edf0895b' THEN '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'::bytea
                END AS undercoin2,
            CASE
                WHEN _base_pool = '\xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7' THEN '\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea
                WHEN _base_pool = '\x7fc77b5c7614e1533320ea6ddc2eb61fa00a9714' THEN '\xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6'::bytea
                END AS undercoin3
        FROM meta_calls
    )

    SELECT *
    FROM plain_poolDeployed
    UNION ALL
    SELECT *
    FROM meta_poolDeployed
),

V2_poolDeployed AS (
    SELECT
        'Factory V2' AS version,
        _name AS name,
        _symbol AS symbol,
        output_0 AS pool_address,
        p."A" AS A,
        p.mid_fee AS mid_fee,
        p.out_fee AS out_fee,
        p.token AS token_address,
        output_0 AS deposit_contract,
        coins[1] AS coin0,
        coins[2] AS coin1,
        coins[3] AS coin2,
        coins[4] AS coin3,
        NULL::bytea AS undercoin0,
        NULL::bytea AS undercoin1,
        NULL::bytea AS undercoin2,
        NULL::bytea AS undercoin3
        -- coins::text AS coins
    FROM curvefi."CurveFactoryV2_evt_CryptoPoolDeployed" p
    LEFT JOIN curvefi."CurveFactoryV2_call_deploy_pool" ON p.evt_tx_hash = call_tx_hash
),

pools AS (
    SELECT *
    FROM regular_poolDeployed

    UNION ALL

    SELECT
        pd.*,
        gauge AS gauge_contract
    FROM V1_poolDeployed pd
    LEFT JOIN curvefi."CurveFactory_evt_LiquidityGaugeDeployed" g ON pd.pool_address = g.pool

    UNION ALL

    SELECT
        pd.*,
        gauge AS gauge_contract
    FROM V2_poolDeployed pd
    LEFT JOIN curvefi."CurveFactoryV2_evt_LiquidityGaugeDeployed" g ON pd.pool_address = g.token
)

SELECT
    version,
    p.name,
    symbol,
    pool_address,
    CASE WHEN namespace IS NULL THEN 'no' ELSE 'yes' END AS decoded,
    namespace AS dune_namespace,
    c.name AS dune_table_name,
    A,
    mid_fee,
    out_fee,
    token_address,
    deposit_contract,
    coin0,
    coin1,
    coin2,
    coin3,
    undercoin0,
    undercoin1,
    undercoin2,
    undercoin3,
    gauge_contract
FROM pools p
LEFT JOIN ethereum.contracts c ON c.address = pool_address
ORDER BY dune_table_name DESC
