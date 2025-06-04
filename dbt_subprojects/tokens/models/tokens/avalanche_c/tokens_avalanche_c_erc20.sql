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
    -- tokens which do not exist in automated tokens.erc20
    (0xfe09e309726fcdb92b05df5f682185a0b0590dd9, 'RIPPED', 18)
    , (0xae64d55a6f09e4263421737397d1fdfa71896a69, 'sGLP', 18)
    , (0x3b9e3b5c616a1a038fdc190758bbe9bab6c7a857, 'UNCX', 18)
    , (0xbacd77ac0c456798e05de15999cb212129d90b70, 'WOOFY', 18)
    , (0x9a4e5e7fbb3bbf0f04b78354aafea877e346ae33, 'sGLP', 18)
    , (0x0b82a1ad2138e9f62454ac41b702b64e0b73d57b, 'sGLP', 18)
)
AS temp_table (contract_address, symbol, decimals)