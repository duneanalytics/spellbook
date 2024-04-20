{{config(
        tags = ['static'],
        schema = 'cex_evms',
        alias = 'addresses',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "avalanche_c", "bnb", "fantom", "optimism", "polygon", "zksync", "zora", "celo", "base"]\',
                                    "sector",
                                    "cex",
                                    \'["hildobby", "soispoke", "web3_data", "msilb7", "Henrystats", "sankinyue"]\') }}')}}

SELECT address, cex_name, distinct_name, added_by, added_date
FROM (VALUES
    -- Binance
    (0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be, 'Binance', 'Binance 1', 'hildobby', date '2022-08-28')
    , (0xd551234ae421e3bcba99a0da6d736074f22192ff, 'Binance', 'Binance 2', 'hildobby', date '2022-08-28')
    , (0x564286362092d8e7936f0549571a803b203aaced, 'Binance', 'Binance 3', 'hildobby', date '2022-08-28')
    , (0x0681d8db095565fe8a346fa0277bffde9c0edbbf, 'Binance', 'Binance 4', 'hildobby', date '2022-08-28')
    , (0xfe9e8709d3215310075d67e3ed32a380ccf451c8, 'Binance', 'Binance 5', 'hildobby', date '2022-08-28')
    , (0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67, 'Binance', 'Binance 6', 'hildobby', date '2022-08-28')
    , (0xbe0eb53f46cd790cd13851d5eff43d12404d33e8, 'Binance', 'Binance 7', 'hildobby', date '2022-08-28')
    , (0xf977814e90da44bfa03b6295a0616a897441acec, 'Binance', 'Binance 8', 'hildobby', date '2022-08-28')
    , (0x001866ae5b3de6caa5a51543fd9fb64f524f5478, 'Binance', 'Binance 9', 'hildobby', date '2022-08-28')
    , (0x85b931a32a0725be14285b66f1a22178c672d69b, 'Binance', 'Binance 10', 'hildobby', date '2022-08-28')
    , (0x708396f17127c42383e3b9014072679b2f60b82f, 'Binance', 'Binance 11', 'hildobby', date '2022-08-28')
    -- Norwegian Block Exchange
    , (0x29af949c3D218C1133bD16257ed029E92deFb168, 'Norwegian Block Exchange', 'Norwegian Block Exchange 1', 'hildobby', date '2023-04-07')
    , (0x8Cad96fB23924Ebc37b8CdAFa8400AD856fE4a2C, 'Norwegian Block Exchange', 'Norwegian Block Exchange 2', 'hildobby', date '2023-04-07')
    , (0xAeB81c391Ac427B6443310fF1cB73a21E071e5ad, 'Norwegian Block Exchange', 'Norwegian Block Exchange 3', 'hildobby', date '2023-04-07')
    , (0x052Ed0aD68Ffc470386FDAb82F7046E0b55FD663, 'Norwegian Block Exchange', 'Norwegian Block Exchange 4', 'hildobby', date '2023-11-22')
    , (0xfACCB74832546a745aaB8Dbd2d155Dc67a222048, 'Norwegian Block Exchange', 'Norwegian Block Exchange 5', 'hildobby', date '2023-11-22')
    , (0x0d019414aC7DD7E8262aE7Dc9EFCC6bDe050b0DD, 'Norwegian Block Exchange', 'Norwegian Block Exchange 6', 'hildobby', date '2023-11-22')
    -- BitVenus
    , (0xe43c53c466a282773f204df0b0a58fb6f6a88633, 'BitVenus', 'BitVenus 1', 'hildobby', date '2023-04-07')
    , (0x2b097741854eedeb9e5c3ef9d221fb403d8d8609, 'BitVenus', 'BitVenus 2', 'hildobby', date '2023-04-07')
    , (0x686b9202a36c09ce8aba8b49ae5f75707edec5fe, 'BitVenus', 'BitVenus 3', 'hildobby', date '2023-04-07')
    , (0xef7a2610a7c9cfb2537d68916b6a87fea8acfec3, 'BitVenus', 'BitVenus 4', 'hildobby', date '2023-04-07')
    , (0x5631aa1fc1868703a962e2fd713dc02cad07c1db, 'BitVenus', 'BitVenus 5', 'hildobby', date '2023-04-07')
    , (0x4785e47ae7061632c2782384da28b9f68a5647a3, 'BitVenus', 'BitVenus 6', 'hildobby', date '2023-04-07')
    ) AS x (address, cex_name, distinct_name, added_by, added_date)