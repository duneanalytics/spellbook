{{
    config(
        schema = 'tokens_arbitrum'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM (VALUES
    (0xa98c94d67d9df259bee2e7b519df75ab00e3e2a8, 'bwAJNA', 18)
    , (0xda492c29d88ffe9b7cbfa6dc068c2f9befae851b, 'CUSDCLP', 18)
    , (0x831b0afaa3b22e1435169c7585ccc1861a2c9cbc, 'fUSDC', 6)
    , (0x763e061856b3e74a6c768a859dc2543a56d299d5, 'tigETH', 18)
    , (0x753d224bcf9aafacd81558c32341416df61d3dac, 'PERP', 18)
    , (0x0d81e50bc677fa67341c44d7eaa9228dee64a4e1, 'BOND', 18)
    , (0x81c958c2cb5158e2b7d3d8a7c41ef2110c0ef98d, 'xFORTUN', 18)
    , (0xb40dbbb7931cfef8be73aeec6c67d3809bd4600b, 'PPO', 18)
    , (0x223738a747383d6f9f827d95964e4d8e8ac754ce, 'auraBAL', 18)
    , (0x2416092f143378750bb29b79ed961ab195cceea5, 'ezETH', 18)
    , (0x13780e6d5696dd91454f6d3bbc2616687fea43d0, 'USTC', 6)
    , (0x9fb9a33956351cf4fa040f65a13b835a3c8764e3, 'MULTI', 18)
    , (0xac9ac2c17cdfed4abc80a53c5553388575714d03, 'ATA', 18)
    , (0x0eab25ecb949827d675864ea7686a8a7efe41116, 'SFT', 18)
    , (0xb165a74407fe1e519d6bcbdec1ed3202b35a4140, 'stataArbUSDT', 6)
    , (0x5298060a95205be6dd4abc21910a4bb23d6dcd8b, 'ROUTE', 18)
    , (0xcf879b434fe68d3d4fe3616582d26537a220f04b, 'PLAY', 18)
    , (0x2dc5dd89a3662567b78fc3a78e1e2c81d9e4d419, 'BANANIA', 18)
    , (0x218fdee44e8e923b500895e324af6c0a2e07195d, 'vrAMM-YFX/USDC', 18)
    , (0x20151ff7fdd720b85063d02081aa5b7876adff7b, 'MASH', 6)
    , (0x5c21f4b87eb5d811c824035bde9de9791766c094, 'WSN', 18)
    , (0x88ec3bfb63f5583bb4127a8d834be87e67908e2c, 'ADAI', 18)
    , (0x7cfadfd5645b50be87d546f42699d863648251ad, 'stataArbUSDCn', 6)
    , (0x6fe14d3cc2f7bddffba5cdb3bbe7467dd81ea101, 'COTI', 18)
    , (0xe2b4179dc78206e98ee3130ff64fc152923f6d23, 'POTION', 10)
    , (0xef888bca6ab6b1d26dbec977c455388ecd794794, 'RGT', 18)
    , (0xb86af5eb59a8e871bfa573fa656123ea86f47c3a, 'CWETHLP', 18)
    , (0x211cc4dd073734da055fbf44a2b4667d5e5fe5d2, 'sUSDe', 18)
    , (0xC3F47f3627305213ADaa021CcCCb61D5987EAa97, 'HRK' , 18)
    , (0x83e1d2310ade410676b1733d16e89f91822fd5c3, 'JitoSOL' , 9)    
    , (0x81b58ae322e933f8238505538a73fe81ad4f2b1e, 'BT' , 18)
    , (0xe405f6384bcd8d44981879599983d92bd9776586, 'UEE' , 9)
    , (0x5e0543f61f94b40c9a5265b5b3a7b35aa8dc6b49, 'AT' , 18)
    , (0x4a2f6ae7f3e5d715689530873ec35593dc28951b, 'wstETH/rETH/cbETH', 18)
    , (0x9791d590788598535278552eecd4b211bfc790cb, 'wstETH-WETH-BPT', 18)
    , (0x502697af336f7413bb4706262e7c506edab4f3b9, 'arbJnrLLP', 18)
    , (0x5fb31318e9a82efcaa2cfefbacf63e85f4dff2f1, 'APT', 12)
    , (0xa37ef01065e0328b50a85256e159b9aaed196e05, 'APT', 24)
    , (0xcfd72be67ee69a0dd7cf0f846fc0d98c33d60f16, 'nUSD-LP', 18)
    , (0xb076f79f8d1477165e2ff8fa99930381fb7d94c1, 'arbMzeLLP', 18)
    , (0x14fbc760efaf36781cb0eb3cb255ad976117b9bd, 'PENDLE-LPT', 18)
    , (0xade4a71bb62bec25154cfc7e6ff49a513b491e81, 'rETH-WETH-BPT', 18)
    , (0x29240aff8c592640a20cbfe0db563a6ffeb12b01, 'APT', 24)
    , (0x0c8972437a38b389ec83d1e666b69b8a4fcf8bfd, 'wstETH/rETH/sfrxETH', 18)
    , (0xd70a52248e546a3b260849386410c7170c7bd1e9, 'nETH-LP', 18)
    , (0x8bc65eed474d1a00555825c91feab6a8255c2107, 'DOLA/USDC BPT', 18)
    , (0x3fd4954a851ead144c2ff72b1f5a38ea5976bd54, 'ankrETH/wstETH-BPT', 18)
    , (0x5402b5f40310bded796c7d0f3ff6683f5c0cffdf, 'sGLP', 18)
) AS temp_table (contract_address, symbol, decimals)
