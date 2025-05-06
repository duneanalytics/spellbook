{{
    config(
        schema = 'tokens_blast'
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
    (0x4300000000000000000000000000000000000004, 'WETH', 18)
    , (0x20fe91f17ec9080e3cac2d688b4ecb48c5ac3a9c, 'YES', 18)
    , (0x491e6de43b55c8eae702edc263e32339da42f58c, 'ESE', 18)
    , (0x5ffd9ebd27f2fcab044c0f0a26a45cb62fa29c06, 'PAC', 18)
    , (0x2416092f143378750bb29b79ed961ab195cceea5, 'ezETH', 18)
    , (0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A, 'weETH', 18)
    , (0x010392305558d58e1Cb0Eec5529a65bf3545f82e, 'MACHI', 18)
    , (0x373318cccC1da7934929d8af23eA17641522206B, 'pUSDB-WETH LP', 18)
    , (0x1a49351bdB4BE48C0009b661765D01ed58E8C2d8, 'YES', 18)
    , (0x58538e6A46E07434d7E7375Bc268D3cb839C0133, 'ENA', 18)
    , (0x73c369F61c90f03eb0Dd172e95c90208A28dC5bc, 'OLA', 18)
    , (0x9FE9991dAF6b9a5d79280F48cbb6827D46DE2EA4, 'HYPE', 9)
    , (0xE070B87c4d88826D4cD1b85BAbE186fdB14CD321, 'CBR', 18)
    , (0x52f847356b38720B55ee18Cb3e094ca11C85A192, 'FNX', 18)
) AS temp_table (contract_address, symbol, decimals)
