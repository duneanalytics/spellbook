{{
    config(
        schema = 'tokens_zksync'
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
    (0xf755cf4f0887279a8bcbe5e39ee062a5b7188401, 'LQTY', 18)
    , (0x6ee46cb7cd2f15ee1ec9534cf29a5b51c83283e6, 'KNC', 18)
    , (0xd7c6210f3d6011d6b1bddfa60440fe763340df4c, 'WAGMI', 18)
    , (0x1bbd33384869b30a323e15868ce46013c82b86fb, 'nETH', 8)
    , (0xa0c1bc64364d39c7239bd0118b70039dbe5bbdae, 'UFI', 18)
    , (0x97003ac71cc4a096e06c73e753d9b84f0039a064, 'POOL', 18)
    , (0x6f1a89c16a49549508a2b6d2ac6f34523aa2a545, 'xcRMRK', 18)
    , (0x668cc2668eeeaf8075d38e72ef54fa546bf3c39c, 'ETHx', 18)
    , (0x3d79f1e3f6afd3f30ea450afffb8632aed59b46f, 'RAISE', 18)
    , (0x2d850f34e957ba3dcbee47fc2c79ff78044fb12e, 'BYN', 18)
    , (0xc5db68f30d21cbe0c9eac7be5ea83468d69297e6, 'rfETH', 18)
    , (0x26b7f317c440e57db2fb4b377a3f1b3bbf5463c7, 'BITCOIN', 18)
    , (0x22d8b71599e14f20a49a397b88c1c878c86f5579, 'eETH', 8)
    , (0x1cf8553da5a75c20cdc33532cb19ef7e3bfff5bc, 'RPL', 18)
    , (0xe0ef1c039a36ec77339e7277ecd4d48e57b61eec, 'ySYNC', 18)
    , (0x9e22d758629761fc5708c171d06c2fabb60b5159, 'WOO', 18)
    , (0x140d5bc5b62d6cb492b1a475127f50d531023803, 'DERI', 18)
) AS temp_table (contract_address, symbol, decimals)
