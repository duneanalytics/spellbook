{{
    config(
        schema = 'tokens_lens'
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
    (0x6bDc36E20D267Ff0dd6097799f82e78907105e2F, 'WGHO', 18)
    , (0xE5ecd226b3032910CEaa43ba92EE8232f8237553, 'WETH', 18)
    , (0x88F08E304EC4f90D644Cec3Fb69b8aD414acf884, 'USDC', 6)
    , (0xB0588f9A9cADe7CD5f194a5fe77AcD6A58250f82, 'BONSAI', 18)
) AS temp_table (contract_address, symbol, decimals)