{{
    config(
        schema = 'tokens_mantle'
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
    (0x19a414a6b1743315c731492cb9b7b559d7db9ab7, 'MoeLP', 18)
    , (0x1a4d4aa3bd8587f6e05cc98cf87954f7d95c11c6, 'MoeLP', 18)
    , (0x30ac02b4c99d140cde2a212ca807cbda35d4f6b5, 'MoeLP', 18)
    , (0x33b1d7cfff71bba9dd987f96ad57e0a5f7db9ac5, 'MoeLP', 18)
    , (0x347bb5065eadd5f7cb5fd0a696137d49f38ac6cb, 'MoeLP', 18)
    , (0x371c7ec6d8039ff7933a2aa28eb827ffe1f52f07, 'JOE', 18)
    , (0x4515a45337f461a11ff0fe8abf3c606ae5dc00c9, 'MOE', 18)
    , (0x4a18891de69124d2853a4e27543edb7e2e001179, 'MoeLP', 18)
    , (0x4e7685df06201521f35a182467feefe02c53d847, 'MoeLP', 18)
    , (0x5126ac4145ed84ebe28cfb34bb6300bcef492bb7, 'MoeLP', 18)
    , (0x562a1a3979a4a10ac2e060cfa4b53cad8011604a, 'MoeLP', 18)
    , (0x763868612858358f62b05691db82ad35a9b3e110, 'MoeLP', 18)
    , (0x7d35ba038df5afde64a1962683ffeb3e150637ff, 'MoeLP', 18)
    , (0x86e3a987187fed135d6d9c114f1857d8144f01e1, 'MoeLP', 18)
    , (0x8e3a13418743ab1a98434551937ea687e451b589, 'MoeLP', 18)
    , (0xb1e695dc6ca41d0dc5030d7e316c879a47fd492a, 'MoeLP', 18)
    , (0xb670d2b452d0ecc468cccfd532482d45dddde2a1, 'MoeLP', 18)
    , (0xc1f43e45f86e7bfb92c3c309b0ef366f9ba33bfa, 'MoeLP', 18)
    , (0xefc38c1b0d60725b824ebee8d431abfbf12bc953, 'MoeLP', 18)
    , (0xfbea6da85f6554fe4d429e9f37f8ba54a9ac94c3, 'MoeLP', 18)
    , (0x5d131cb99ce5642f3d539417a187a93eeae48177, 'S*USDC', 6)
    , (0xe1152564ed7b59e01915fc95bbf87cf9b6636fe6, 'S*USDT', 6)
) AS temp_table (contract_address, symbol, decimals)
