{{config(alias='alameda',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["ilemi"]\') }}')}}

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at
FROM (VALUES
    -- alameda, source Larry (theBlock) https://docs.google.com/spreadsheets/d/1lBNOmTZnKTkiPCROX9NfGqqd8S-1XNChb3Dh_l_KSW0
    (array({ allEVMchains() }),'0xe31a9498a22493ab922bc0eb240313a46525ee0a','Alameda Research 1','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x712d0f306956a6a4b4f9319ad9b9de48c5345996','Alameda Research 2','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x93c08a3168fc469f3fc165cd3a471d19a37ca19e','Alameda Research 3','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0xca436e14855323927d6e6264470ded36455fc8bd','Alameda Research 4','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x83a127952d266a6ea306c40ac62a4a70668fe3bd','Alameda Research (FTX Deposit) 5','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0xc5ed2333f8a2c351fca35e5ebadb2a82f5d254c3','Alameda Research 6','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x89183c0a8965c0299997be9af700a801bdccc2da','Alameda Research (FTX Deposit) 7','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0xe5d0ef77aed07c302634dc370537126a2cd26590','Alameda Research 8','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x5d13f4bf21db713e17e04d711e0bf7eaf18540d6','Alameda Research 9','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x882a812d75aee53efb8a144f984b258b6c4807f0','Alameda Research 10','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0xbefe4f86f189c1c817446b71eb6ac90e3cb68e60','Alameda Research 11','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0xb78e90e2ec737a2c0a24d68a0e54b410fff3bd6b','Alameda Research 12','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x964d9d1a532b5a5daeacbac71d46320de313ae9c','Alameda Research 13','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0xfa453aec042a837e4aebbadab9d4e25b15fad69d','Alameda Research 14','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x4deb3edd991cfd2fcdaa6dcfe5f1743f6e7d16a6','Alameda Research 15','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x477573f212a7bdd5f7c12889bd1ad0aa44fb82aa','Alameda Research 16','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0xce31190a03fc3c5f23167e88e75066824823222d','Alameda Research 17','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x60009b78da046ac64ef789c29ca05b79cdf73c10','Alameda Research (Genesis Deposit) 18','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x73c0ae50756c7921d1f32ada71b8e50c5de7ff9c','Alameda Research 19','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x60ae578abdfded1fb0555f54148fdd7b400a34ed','Alameda Research 20','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0xa726c00cda1f60aaab19bc095d02a46556837f31','Alameda Research 21','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0xa6e683d5dccce898f16bb48071f08f2304c8ba09','Alameda Research 22','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x0c0fe4e0236480e16b679ee1fd0c5247f9cf35f0','Alameda Research (Huobi deposit) 23','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x0f4ee9631f4be0a63756515141281a3e2b293bbe','Alameda Research 24','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x97137466bc8018531795217f0ecc4ba24dcba5c1','Alameda Research 25','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x84d34f4f83a87596cd3fb6887cff8f17bf5a7b83','Alameda Research 26','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x78835265ac857bf3420830c71987b1a55f73c2dc','Alameda Research 27','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x4c8cfe078a5b989cea4b330197246ced82764c63','Alameda Research 28','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x073dca8acbc11ffb0b5ae7ef171e4c0b065ffa47','Alameda Research 29','alameda','ilemi','static',timestamp('2022-11-06'),now())
    , (array({ allEVMchains() }),'0x3507e4978e0eb83315d20df86ca0b976c0e40ccb','Alameda Research (Binance Deposit) 30','alameda','ilemi','static',timestamp('2022-11-06'),now())
) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at)