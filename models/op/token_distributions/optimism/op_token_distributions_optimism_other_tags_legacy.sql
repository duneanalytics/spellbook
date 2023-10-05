{{ config(
	tags=['legacy'],
	
    alias = alias('other_tags', legacy_model=True)
    )
}}

-- These are wallets that we don't want to track distributions for, but we know what they are, so don't want to keep re-checking them over and over again

WITH tagged_wallets AS (
SELECT distinct address, address_name
FROM (
SELECT LOWER(address) AS address, cast(address_name as varchar(250)) AS address_name
    , ROW_NUMBER() OVER (PARTITION BY address ORDER BY cast(address_name as varchar(250)) ) AS rnk
FROM (values
             ('0x030058ac851ba6f282ec0e717a7ed577d09dff0b','Perp Foundation')
            ,('0x283c280ef8c42e2775d12aa804f5e053475d5397','Perp Foundation')
            ,('0x1cb769d547280ddca69c932b1a822d51eca58b3d','Perp Foundation')
            ,('0x675e328b73e15cd41acd2828b83b417687c36bba','Perp Foundation')
            ,('0x94934851b2c6a066f9e5acb353bce4bc7fcdcdbd','Perp Foundation')
            ,('0xe77dd81813bd06d74a30d1b2c07418906456498e','Perp Foundation')
            ,('0x70332de459d5554a9a310a7c4f7c6069c73604f9','Perp Foundation')
            ,('0x3478d1ba5bc6c0e662dec109c70c0065dad5f5be','Perp Foundation')
            ,('0xd8bcd1e2f0e96a3402fd678b0b2eaceb80e1c226','Perp Foundation')
            ,('0x5366910021b3a66e84f9b974415ce2c067729c04','Perp Foundation')
            ,('0x6fe5c2b4a4c9268d4e08510fbfd042ebdf47ff81','Perp Foundation')
            ,('0xff05618995e03dffd32c6ff819d1c74b2ff04fa9','Perp Foundation')
            ,('0xb3208f9f07b129b825f0d00d5bb79eec69db5126','Perp Foundation')
            ,('0x148824400a454dc00093e6604b6b9a6a208bf3cf','Perp Foundation')
            ,('0x6e7aa93aabe2d0019e2a096d227e56e3392b8ec5','Perp Foundation')
            ,('0x26cb31fde1015080ce5cc9b0866219fd6153acc7','Perp Foundation')
            ,('0xe5ee79d5cd1cb3d512ec5bf4e29dbb1183bb0187','Perp Foundation')
            ,('0x9578f46ce562760e2ce2e8b99b746de68ce10d0f','Perp Foundation')
            ,('0x478946a8e7b62e5dd15e3786932c5822397a1a8f','Perp Foundation')
            ,('0x7af8dff16406499b61c9b5fa7c0f9f3b623f8734','Perp Foundation')
            ,('0x9c1e0c67aa30c063f341885b12cb81cc94613fc7','Perp Foundation')
            ,('0xf8489bcef22d3282bb884b9e9cc708bb465c075e','Perp Foundation')
            ,('0x4cd804c696f54c419b75fc241a17c512bfb13df4','Perp Foundation')
            ,('0x7702dc73e8f8d9ae95cf50933adbee68e9f1d725','dForceOP Pool')
            ,('0xc5785b0ce1095213465a4a4f28c19269cb4b35ec','Lyra Unknown')
            ,('0x9fa23d27bc93533cd29e6038275611c829813147','Aelin user')
            ,('0x897a7f6af47881c62d7ca7e3dc5bd8a1341ad8ae', 'xToken LP')
            ,('0x68f5c0a2de713a54991e01858fd27a3832401849', 'xToken LP ')
            ,('0xcc98cfdc5f5480d8dd0a0d0a7f80506eb30d5159', 'xToken LP')
            ,('0x574a21fe5ea9666dbca804c9d69d8caf21d5322b', 'Rubicon LP')
            ,('0x1111111254760f7ab3f16433eea9304126dcd199', '1inch Swap')
            ,('0x1111111254EEB25477B68fb85Ed929f73A960582', '1inch Swap')
            
    ) a (address, address_name)
    ) b
    WHERE rnk = 1 --check to prevent duplicates
)

SELECT *
FROM (
    SELECT 'Other' as label, address, address_name
    FROM tagged_wallets
    UNION ALL
    SELECT 'Other',address, cex_name
    FROM {{ ref('cex_optimism_addresses_legacy') }}
    )
WHERE address NOT IN (SELECT address FROM {{ ref('op_token_distributions_optimism_project_wallets_legacy') }}) --not already tagged
