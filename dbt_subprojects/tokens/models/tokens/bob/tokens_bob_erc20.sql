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
FROM (VALUES
    (0x6c851f501a3f24e29a8e39a29591cddf09369080, 'DAI', 18),
    (0xf3107eec1e6f067552c035fd87199e1a5169cb20, 'DLLR', 18),
    (0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000, 'ETH', 18),
    (0xc96de26018a54d51c097160568752c4e3bd6c364, 'FBTC', 8),
    (0xc4a20a608616f18aa631316eeda9fb62d089361e, 'FRAX', 18),
    (0x15e35b19ad29c512103eaabb55154ef0ee6ca661, 'FXS', 18),
    (0xb5686c4f60904ec2bda6277d6fe1f7caa8d1b41a, 'rETH', 18),
    (0xb7eae04b995b3b365040dee99795112add43afa0, 'sFRAX', 18),
    (0x249d2952d1c678843e7cd7bf654efcec52f2f9e8, 'sfrxETH', 18),
    (0xba20a5e63eeefffa6fd365e7e540628f8fc61474, 'SOV', 18),
    (0x96147a9ae9a42d7da551fd2322ca15b71032f342, 'STONE', 18),
    (0xf14e82e192a36df7d09fe726f6ecf70310f73438, 'T', 18),
    (0xbba2ef945d523c4e2608c9e1214c2cc64d4fc2e2, 'tBTC', 18),
    (0x665060707c3ea3c31b3eabad7f409072446e1d50, 'TRB', 18),
    (0xe75d0fb2c24a55ca1e3f96781a2bcc7bdba058f0, 'USDC', 6),
    (0x05d032ac25d322df992303dca074ee7392c117b9, 'USDT', 6),
    (0x03c7054bcb39f7b2e5b2c7acb37583e32d70cfa3, 'WBTC', 8),
    (0x85008ae6198bc91ac0735cb5497cf125ddaac528, 'wstETH', 18)
) AS temp_table (contract_address, symbol, decimals)
