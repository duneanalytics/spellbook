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
-- and adds version information from the centralized source

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
            {%- set deployments = get_official_safe_deployments() -%}
            {%- for addr, info in deployments.items() %}
            WHEN LOWER(address) = LOWER('{{ addr }}') THEN '{{ info.version }}{{ " - " ~ info.note if info.get("note") else "" }}'
            {%- endfor %}
            ELSE 'unknown'
        END as safe_version,
        CASE 
            {% set official_addresses = get_official_safe_addresses() %}
            WHEN LOWER(address) IN (
                {%- for addr in official_addresses %}
                LOWER('{{ addr }}'){{ "," if not loop.last else "" }}
                {%- endfor %}
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