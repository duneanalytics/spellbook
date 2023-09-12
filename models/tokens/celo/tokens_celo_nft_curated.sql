{{
    config(
        tags=['static', 'dunesql'],
        alias = alias('nft_curated'),
        materialized = 'table',
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "tokens",
                                    \'["tomfutago"]\') }}'
        )
}}

select contract_address, name, symbol
from (
    values
    (0xB7F88C6e7d9EfC7FD9b587684DfC8B4C4422855E, 'CeloPunks', 'CPUNKS'),
    (0x1eCD77075F7504bA849d47DCe4cdC9695f1FE942, 'CeloApes', 'CAK'),
    (0x6Fc1C8d59FdC261c55273f9b8e64B7E88C45E208, 'CeloToadzNFT', 'CTOADZ'),
    (0x501F7Ea7B1aA25fF7D2feB3a2e96979ba754204B, 'CeloShapes', 'CSHAPE'),
    (0x517bCe2DdBc21b9A8771Dfd3Db40404BDEF1272D, 'MooPunks', 'MPUNK'),
    (0x0C69fAb99e51b6C2e4a1cAE49B123bbbe94a56cD, 'CeloPunks Christmas Edition', 'CHRISTMASPUNK'),
    (0xc4ea80deCA2415105746639eC16cB0cF8378996A, 'Daopolis', 'DAOS'),
    (0x50826Faa5b20250250E09067e8dDb1AFa2bdf910, 'WomxnOfCelo', 'WMXN'),
    (0x8237f38694211F25b4c872F147F027044466Fa80, 'Nomstronaut', 'Nomstronaut'),
    (0x660C6442F01c75fE1e389A607A4a7662342f2FD2, 'CeloPaints', 'CPAINT'),
    (0xAc80c3c8b122DB4DcC3C351ca93aC7E0927C605d, 'Celostrials', 'NFET'),
    (0xB346E32c9c5196A580F7466a039D902b505Ae7d4, 'Carbonized Celostrials', 'NFET02'),
    (0xb69948766ADeF1e85291c322114c3d98eEbA8695, 'ChinChilla_Gang_Airdrop', 'GANGAIRDROP'),
    (0xc8DF51073CD581902b4fb50131d31f29343131F0, 'ChinChillaGang', 'GANG'),
    (0xA83a8C2f6a762F8BE760A0f45540E3864750dE0F, 'SToadzNFT', 'STOADZ'),
    (0x1F25F8Df9E33033668d6F04DAE0bDE4854E9F1A5, 'KnoxerNFT', 'KNX_NFT'),
    (0xe2BbaE9A94513736f8F4Aed5D399bd6B1e0Bfb48, 'CeloMonkeyBusiness', 'CeloMBS'),
    (0xa4BC4EfBA9D81962325B02eFE91772c7B4aaB31D, 'CeloKatzWarriors', 'CKW'),
    (0xfA83588c92a353fba568D7C25A32E599FEa35763, 'NaviKatzWarriors', 'NKW'),
    (0xc017019e7B1566900553987ac1D9b25D126dA16C, 'CeloPunksCeloConnect', 'CCONNECTPUNKS'),
    (0x56546DAF99C69c0F6271FA287b30A1946cA466f0, 'CeloErectus', 'CER'),
    (0x6eBc8879Ee57334B14a9B1e3A2fDe55E172193C6, 'CeloWhales', 'CWHALE'),
    (0x91d0e3A2BCCf036bcf61536D2BDFD47D79946aAE, 'Shiki', 'SHIKI'),
    (0x8481d620CBa0Dc4a4421D1015DAbc60Ec55d6172, 'Ariart', 'ARIa'),
    (0x850F0b409cF1b8FB5E03821870fBf9F119Dbe52e, 'Impact Quest - CELO - Save2Savee', 'ImpactQuest'),
    (0x640a33Ab2102ba5Dc802fac852A87BC2F2770F8a, 'Impact Quest 2', 'ImpactQuest2'),
    (0x9DeF34B1ace8C135029AEC19f9744762A572b2c3, 'Toucan Protocol: Retirement Certificates for Tokenized Carbon Offsets', 'TOUCAN-CERT'),
    (0x8E19B1Ad222147193a9aF9B0A844B680721a007D, 'Wheelcoin Vehicles', 'WHL-VHC'),
    (0xD1240Efd4C8B90A9b5B126eD1Dd8Ca099F832342, 'First WheelCoin Collection', 'WHL1'),
    (0x90e723DaaeE9FDcD95d5353f6bfC464Ff2d4591a, 'CELOAKS', 'CELOAKS'),
    (0x376f5039Df4e9E9c864185d8FaBad4f04A7E394A, 'Celo Domain Name', 'CDN'),
    (0xe41CE70624CFECb425f9Ef0049784f2ed6Ea4019, 'Stewards for Change', 'STWRD')
) as temp_table (contract_address, name, symbol)
