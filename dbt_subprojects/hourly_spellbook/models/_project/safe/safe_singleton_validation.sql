{{ config(
    schema = 'safe',
    alias = 'singleton_validation',
    materialized = 'table',
    post_hook='{{ expose_spells(\'["ethereum","optimism","gnosis","polygon","bnb","avalanche_c","fantom","arbitrum","celo","base","zksync","zkevm","scroll","linea","mantle","blast","worldchain","ronin","unichain","berachain"]\',
                                "project",
                                "safe",
                                \'["safehjc"]\') }}'
) }}

-- This model validates discovered singleton addresses against official Safe deployments
-- and adds version information

WITH all_singletons AS (
    -- Aggregate all discovered singleton addresses from each network
    {% set networks = ['arbitrum','avalanche_c','base','berachain','blast','bnb','celo','ethereum','fantom','gnosis','linea','mantle','optimism','polygon','ronin','scroll','unichain','worldchain','zkevm','zksync'] %}
    {% for network in networks %}
    SELECT 
        '{{ network }}' as blockchain,
        address,
        'discovered' as source_type
    FROM {{ ref('safe_' ~ network ~ '_singletons') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
),

enriched_singletons AS (
    SELECT 
        blockchain,
        address,
        source_type,
        CASE 
            -- v1.5.0 Safe contracts
            WHEN LOWER(address) = LOWER('0xFf51A5898e281Db6DfC7855790607438dF2ca44b') THEN 'v1.5.0 - Safe'
            WHEN LOWER(address) = LOWER('0xEdd160fEBBD92E350D4D398fb636302fccd67C7e') THEN 'v1.5.0 - SafeL2'
            -- v1.4.0 and v1.4.1 Safe contracts (same singleton addresses)
            WHEN LOWER(address) = LOWER('0x41675C099F32341bf84BFc5382aF534df5C7461a') THEN 'v1.4.x - Safe'
            WHEN LOWER(address) = LOWER('0x29fcB43b46531BcA003ddC8FCB67FFE91900C762') THEN 'v1.4.x - SafeL2'
            -- v1.3.0 Safe contracts  
            WHEN LOWER(address) = LOWER('0xd9Db270c1B5E3Bd161E8c8503c55cEABeE709552') THEN 'v1.3.0 - GnosisSafe'
            WHEN LOWER(address) = LOWER('0x3E5c63644E683549055b9Be8653de26E0B4CD36E') THEN 'v1.3.0 - GnosisSafeL2'
            -- v1.2.0
            WHEN LOWER(address) = LOWER('0x6851D6fDFAfD08c0295C392436245E5bc78B0185') THEN 'v1.2.0 - GnosisSafe'
            -- v1.1.1
            WHEN LOWER(address) = LOWER('0x34CfAC646f301356fAa8B21e94227e3583Fe3F5F') THEN 'v1.1.1 - GnosisSafe'
            -- v1.0.0
            WHEN LOWER(address) = LOWER('0xb6029EA3B2c51D09a50B53CA8012FeEB05bDa35A') THEN 'v1.0.0 - GnosisSafe'
            ELSE 'unknown'
        END as safe_version,
        CASE 
            WHEN LOWER(address) IN (
                -- v1.5.0
                LOWER('0xFf51A5898e281Db6DfC7855790607438dF2ca44b'),
                LOWER('0xEdd160fEBBD92E350D4D398fb636302fccd67C7e'),
                -- v1.4.x
                LOWER('0x41675C099F32341bf84BFc5382aF534df5C7461a'),
                LOWER('0x29fcB43b46531BcA003ddC8FCB67FFE91900C762'),
                -- v1.3.0
                LOWER('0xd9Db270c1B5E3Bd161E8c8503c55cEABeE709552'),
                LOWER('0x3E5c63644E683549055b9Be8653de26E0B4CD36E'),
                -- v1.2.0
                LOWER('0x6851D6fDFAfD08c0295C392436245E5bc78B0185'),
                -- v1.1.1
                LOWER('0x34CfAC646f301356fAa8B21e94227e3583Fe3F5F'),
                -- v1.0.0
                LOWER('0xb6029EA3B2c51D09a50B53CA8012FeEB05bDa35A')
            ) THEN true
            ELSE false
        END as is_official
    FROM all_singletons
)

SELECT 
    blockchain,
    address,
    safe_version,
    is_official,
    CASE 
        WHEN safe_version LIKE '%L2%' THEN 'L2'
        WHEN safe_version != 'unknown' THEN 'L1'
        ELSE 'unknown'
    END as singleton_type,
    COUNT(*) OVER (PARTITION BY address) as chains_deployed_on
FROM enriched_singletons
ORDER BY is_official DESC, safe_version DESC, blockchain