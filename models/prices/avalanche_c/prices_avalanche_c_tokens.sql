{{ config(
        schema='prices_avalanche_c',
        alias = 'tokens',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}
SELECT
    token_id
    , blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES

    ('aave-new','avalanche_c','AAVE.e',0x63a72806098bd3d9520cc43356dd78afe5d386d9,18),
    ('axlusdt-axelar-usd-tether', 'avalanche_c', 'axlUSDT', 0xf976ba91b6bb3468c91e4f02e68b37bc64a57e66, 6),
    ('axlatom-axelar-wrapped-atom', 'avalanche_c', 'axlATOM', 0x80d18b1c9ab0c9b5d6a6d5173575417457d00a12, 6),
    ('frax-frax', 'avalanche_c', 'FRAX', 0xd24c2ad096400b6fbcd2ad8b24e7acbc21a1da64, 18),
    ('fxs-frax-share', 'avalanche_c', 'FXS', 0x214db107654ff987ad859f34125307783fc8e387, 18),
    ('avax-avalanche', 'avalanche_c', 'WAVAX', 0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7, 18),
    ('dai-dai', 'avalanche_c', 'DAI', 0xd586e7f844cea2f87f50152665bcbc2c279d8d70, 18),
    ('usdc-usd-coin', 'avalanche_c', 'USDC', 0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e, 6),
    ('usdt-tether', 'avalanche_c', 'USDT', 0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7, 6),
    ('wbtc-wrapped-bitcoin', 'avalanche_c', 'WBTC', 0x50b7545627a5162f82a992c33b87adc75187b218, 8),
    ('bets-betswirl', 'avalanche_c', 'BETS', 0xc763f8570a48c4c00c80b76107cbe744dda67b79, 18),
    ('thor-thor', 'avalanche_c', 'THOR', 0x8f47416cae600bccf9530e9f3aeaa06bdd1caa79, 18),
    ('weth-weth', 'avalanche_c', 'WETH.e', 0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab, 18),
    ('btcb-bitcoin-avalanche-bridged-btcb', 'avalanche_c', 'BTC.b', 0x152b9d0fdc40c096757f570a51e494bd4b943e50,8),
    ('woo-wootrade', 'avalanche_c', 'WOO.e', 0xabc9547b534519ff73921b1fba6e672b5f58d083, 18),
    ('usdt-tether', 'avalanche_c', 'USDT.e', 0xc7198437980c041c805a1edcba50c1ce5db95118, 6),
    ('usdc-usd-coin', 'avalanche_c', 'USDC.e', 0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664, 6),
    ('savax-benqi-liquid-staked-avax', 'avalanche_c', 'sAVAX', 0x2b2c81e08f1af8835a78bb2a90ae924ace0ea4be, 18),
    ('mimatic-mimatic', 'avalanche_c', 'MIMATIC', 0x3b55e45fd6bd7d4724f5c47e0d1bcaedd059263e, 18),
    ('mim-magic-internet-money','avalanche_c','MIM',0x130966628846bfd36ff31a822705796e8cb8c18d,18),
    ('joe-trader-joe','avalanche_c','JOE',0x6e84a6216ea6dacc71ee8e6b0a5b7322eebc0fdd,18),
    ('time-wonderland','avalanche_c','TIME',0xb54f16fb19478766a268f172c9480f8da1a7c9c3,9),
    ('pizza-pizza-game','avalanche_c','PIZZA',0x6121191018baf067c6dc6b18d42329447a164f05,18),
    ('ptp-platypus-finance','avalanche_c','PTP',0x22d4002028f537599be9f666d1c4fa138522f9c8,18),
    ('grape-grape-finance','avalanche_c','GRAPE',0x5541d83efad1f281571b343977648b75d95cdac2,18),
    ('tusd-trueusd','avalanche_c','TUSD',0x1c20e891bab6b1727d14da358fae2984ed9b59eb,18),
    ('xen-xen-crypto', 'avalanche_c', 'XEN', 0xC0C5AA69Dbe4d6DDdfBc89c0957686ec60F24389, 18),
    ('link-chainlink', 'avalanche_c', 'LINK.e', 0x5947bb275c521040051d82396192181b413227a3, 18),
    ('mimatic-mimatic', 'avalanche_c', 'MAI', 0x5c49b268c9841aff1cc3b0a418ff5c3442ee3f3b, 18),
    ('luna-luna-wormhole', 'avalanche_c', 'LUNA', 0x70928e5b188def72817b7775f0bf6325968e563b, 6),
    ('ust-terrausd-wormhole', 'avalanche_c', 'UST', 0xb599c3590f42f8f995ecfa0f85d2980b76862fc1, 6),
    ('ico-axelar', 'avalanche_c', 'AXL', 0x44c784266cf024a60e8acf2427b9857ace194c5d, 6),
    ('grain-granary','avalanche_c','GRAIN',0x9df4ac62f9e435dbcd85e06c990a7f0ea32739a9,18),
    ('oath-oath','avalanche_c','OATH',0x2c69095d81305f1e3c6ed372336d407231624cea,18),
    ('fld-fold','avalanche_c','FLD',0x88F89BE3E9b1dc1C5F208696fb9cABfcc684bD5F,18),
    ('egs-emingunsirer','avalanche_c','EGS',0xc92f165f5e20979576A7Ba48f16eb45361c078a2,18), 
    ('coq-coq-inu','avalanche_c','COQ',0x420fca0121dc28039145009570975747295f2329,18),
    ('grt-the-graph','avalanche_c','GRT',0x8a0cac13c7da965a312f08ea4229c37869e85cb9,18),
    ('uni-uniswap','avalanche_c','UNI',0x8ebaf22b6f053dffeaf46f4dd9efa95d89ba8580,18),
    ('mkr-maker','avalanche_c','MKR',0x88128fd4b259552a9a1d457f435a6527aab72d42,18),
    ('busd-binance-usd','avalanche_c','BUSD',0x9c9e5fd8bbc25984b178fdce6117defa39d2db39,18),
    ('comp-compoundd','avalanche_c','COMP',0xc3048e19e76cb9a3aa9d77d8c03c29fc906e2437,18),
    ('3ull-playa3ull-games','avalanche_c','3ULL',0xa77e70d0af1ac7ff86726740db1bd065c3566937,18),
    ('husky-husky-avax','avalanche_c','HUSKY',0x65378b697853568da9ff8eab60c13e1ee9f4a654,18),
    ('xava-avalaunch','avalanche_c','XAVA',0xd1c3f94de7e5b45fa4edbba472491a9f4b166fc4,18),
    ('qi-benqi','avalanche_c','QI',0x8729438eb15e2c8b576fcc6aecda6a148776c0f5,18),
    ('wolf-landwolf-on-avax','avalanche_c','WOLF',0x4f94b8aef08c92fefe416af073f1df1e284438ec,18),
    ('meat-meat','avalanche_c','MEAT',0x47c3118Ad183712Acd42648e9E522e13690f29a0,6),
    ('loong-loong-2024','avalanche_c','LOONG',0x5A2C298902f5ba4C3d8B88f9F7cf8f5be72303B9,18),
    ('micro-micro-coq','avalanche_c','MICRO',0xeFD6aA06EB95e0ab23dE9aC0977d870888b89A71,18),
    ('willy-willy','avalanche_c','WILLY',0x403b78F9F817a55b66030c7B2ddab063D28c2c0C,18),
    ('nochill-avax-has-no-chill','avalanche_c','NOCHILL',0xAcFb898Cff266E53278cC0124fC2C7C94C8cB9a5,18),
    ('bear-bear-coin-on-avax','avalanche_c','BEAR',0x19c79F282d151995D91f6dbdda2739701F9C47AA,18),
    ('tech-tech','avalanche_c','TECH',0x5Ac04b69bDE6f67C0bd5D6bA6fD5D816548b066a,18),
    ('alot-dexalot','avalanche_c','ALOT',0x093783055F9047C2BfF99c4e414501F8A147bC69,18),
    ('majin-majin','avalanche_c','MAJIN',0x73f49d00AC1B520F94d11248808c40774aeb0802,18),
    --('snowy-fantastic-protocol-snowy-token','avalanche_c','SNOWY',0x6da767394f079c1524B3e797D2110C23c3ecf322,18),
    ('monk-monkoshi','avalanche_c','MONK',0x7a5424A1AfB6C786f5dd603847683980Fdb2cda7,18),
    ('man-man-man-man','avalanche_c','MAN',0xBEd472c62A5Aa4dD734B6bF5EbA1413d86AAbA6b,18),
    ('bobs-bobs','avalanche_c','BOBS',0xf5f3216E9fed36F8cCf08D310FEc6FBf7f06200f,18),
    ('roco-roco-finance','avalanche_c','ROCO',0xb2a85C5ECea99187A977aC34303b80AcbDdFa208,18)
) as temp (token_id, blockchain, symbol, contract_address, decimals)
