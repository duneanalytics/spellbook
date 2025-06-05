{{
    config(
        schema = 'tokens_avalanche_c'
        ,alias = 'erc20'
        ,tags=['static']
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
    -- tokens which do not exist in automated tokens.erc20, edge cases only
    (0xfe09e309726fcdb92b05df5f682185a0b0590dd9, 'RIPPED', 18)
    , (0xae64d55a6f09e4263421737397d1fdfa71896a69, 'sGLP', 18)
    , (0x3b9e3b5c616a1a038fdc190758bbe9bab6c7a857, 'UNCX', 18)
    , (0xbacd77ac0c456798e05de15999cb212129d90b70, 'WOOFY', 18)
    , (0x9a4e5e7fbb3bbf0f04b78354aafea877e346ae33, 'sGLP', 18)
    , (0x05b0def5c00ba371683d7035934bcf82b737c364, 'KINGSHIT.x', 18)
    , (0x0b82a1ad2138e9f62454ac41b702b64e0b73d57b, 'sGLP', 18)
    , (0x0cd741f007b417088ca7f4392e8d6b49b4f7a975, 'KINGSHIT', 18)
    , (0x14fcd42fddf4758569793330d1e2dc9e5cd208c2, 'WINE', 18)
    , (0x245b532ad64c7fbfeec9aa42f37291b183cea91b, 'SWOL', 18)
    , (0x459c9a598d6b82863d933df4b6d8f9f2ddfe1b75, 'Monkes', 18)
    , (0x77fbb8760c9be73205296ed1ef8aa5f719a0407d, 'PENGUIN', 18)
    , (0x7861ea8f2c99b087dbbc7f28b6a80a4fa454d223, 'NEKO', 18)
    , (0xa27ed4395d816f4a4a3f9f0145d22c601d97958e, 'GEM', 18)
    , (0xa874977c4fd7a49c9c01851d2bb7ee5e2abeb815, 'WINE', 18)
    , (0xb3da885f7d73219e4f360de5ef07b94fa7ed2fc6, 'FROG', 18)
    , (0xd3955f45499bdf96e33b9f38b8a461a3d448d47b, 'DINOS404', 18)
    , (0xd921cc30f948d747cfdc27bd213b7f80e5a6375f, 'RIB404', 18)
    , (0xf11ed22f65c2b77b2d76b83c3ac8fb3cdc20a20a, 'Enigma404', 18)
    , (0xf6c95c3a750cc7f6a8c96d9b08cc132a44c7bd72, '$SICKO', 18)
    , (0xfd0cae9ff7a7a0c88c7675fe1f87c573643b78df, 'ZEPHYRA', 18)
) AS temp_table (contract_address, symbol, decimals)
