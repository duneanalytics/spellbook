{{
    config(
        schema = 'tokens_bob'
        ,alias = 'erc20'
        ,tags = ['static']
        ,materialized = 'table'
    )
}}

SELECT
    contract_address
    , symbol
    , decimals
FROM 
(
    VALUES
    -- placeholder rows to give example of format, tokens missing in automated tokens.erc20
    (0xc4a20a608616f18aa631316eeda9fb62d089361e, 'FRAX', 18)
    , (0xb7eae04b995b3b365040dee99795112add43afa0, 'sFRAX', 18)
    , (0x249d2952d1c678843e7cd7bf654efcec52f2f9e8, 'sfrxETH', 18)
    , (0xf14e82e192a36df7d09fe726f6ecf70310f73438, 'T', 18)
    , (0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000, 'ETH', 18)
) AS temp_table (contract_address, symbol, decimals)
