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
    , (0x4300000000000000000000000000000000000003, 'USDB', 18)
    , (0xb1a5700fA2358173Fe465e6eA4Ff52E36e88E2ad, 'BLAST', 18)
    , (0x20fe91f17ec9080e3cac2d688b4ecb48c5ac3a9c, 'YES', 6)
    , (0x491e6de43b55c8eae702edc263e32339da42f58c, 'ESE', 18)
    , (0x5ffd9ebd27f2fcab044c0f0a26a45cb62fa29c06, 'PAC', 18)
    , (0x2416092f143378750bb29b79ed961ab195cceea5, 'ezETH', 18)
    , (0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A, 'weETH', 18)
) AS temp_table (contract_address, symbol, decimals)
