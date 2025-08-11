{% macro get_official_safe_addresses() %}
    {#- 
    Returns a list of all official Safe singleton addresses
    Used to filter discovered singletons to only include official deployments
    
    Official Safe Singleton Addresses by Version:
    ============================================
    v1.0.0 - GnosisSafe:       0xb6029EA3B2c51D09a50B53CA8012FeEB05bDa35A
    v1.1.1 - GnosisSafe:       0x34CfAC646f301356fAa8B21e94227e3583Fe3F5F
    v1.2.0 - GnosisSafe:       0x6851D6fDFAfD08c0295C392436245E5bc78B0185
    v1.3.0 - GnosisSafe:       0xd9Db270c1B5E3Bd161E8c8503c55cEABeE709552
    v1.3.0 - GnosisSafeL2:     0x3E5c63644E683549055b9Be8653de26E0B4CD36E
    v1.4.x - Safe:             0x41675C099F32341bf84BFc5382aF534df5C7461a
    v1.4.x - SafeL2:           0x29fcB43b46531BcA003ddC8FCB67FFE91900C762
    v1.5.0 - Safe:             0xFf51A5898e281Db6DfC7855790607438dF2ca44b
    v1.5.0 - SafeL2:           0xEdd160fEBBD92E350D4D398fb636302fccd67C7e
    
    Source: https://github.com/safe-global/safe-deployments
    #}
    {%- set addresses = [
        '0xb6029EA3B2c51D09a50B53CA8012FeEB05bDa35A',
        '0x34CfAC646f301356fAa8B21e94227e3583Fe3F5F',
        '0x6851D6fDFAfD08c0295C392436245E5bc78B0185',
        '0xd9Db270c1B5E3Bd161E8c8503c55cEABeE709552',
        '0x3E5c63644E683549055b9Be8653de26E0B4CD36E',
        '0x41675C099F32341bf84BFc5382aF534df5C7461a',
        '0x29fcB43b46531BcA003ddC8FCB67FFE91900C762',
        '0xFf51A5898e281Db6DfC7855790607438dF2ca44b',
        '0xEdd160fEBBD92E350D4D398fb636302fccd67C7e'
    ] -%}
    {{ return(addresses) }}
{% endmacro %}

{% macro safe_singletons_modern_validated(blockchain, sources_list) %}
-- Fetch all known singleton addresses used via the factory
-- FILTERED to only include official Safe deployments
{%- set official_addresses = get_official_safe_addresses() -%}
WITH all_singletons AS (
    {%- for source_info in sources_list %}
    select distinct singleton as address 
    from {{ source('gnosis_safe_' ~ blockchain, source_info) }}
    {%- if not loop.last %}
    
    union 
    {% endif %}
    {%- endfor %}
)
SELECT DISTINCT address
FROM all_singletons
WHERE LOWER(address) IN (
    {%- for addr in official_addresses %}
    LOWER('{{ addr }}'){% if not loop.last %},{% endif %}
    {%- endfor %}
)
{% endmacro %}

{% macro safe_singletons_legacy_validated(blockchain, legacy_sources, modern_sources) %}
-- Fetch all known singleton/mastercopy addresses used via factories
-- FILTERED to only include official Safe deployments
{%- set official_addresses = get_official_safe_addresses() -%}
WITH all_singletons AS (
    {%- for source_info in legacy_sources %}
    select distinct {{ source_info.column }} as address 
    from {{ source('gnosis_safe_' ~ blockchain, source_info.table) }}
    {%- if not loop.last or modern_sources|length > 0 %}
    
    union 
    {% endif %}
    {%- endfor %}
    
    {%- if modern_sources|length > 0 %}
    {%- for source_info in modern_sources %}
    select distinct singleton as address 
    from {{ source('gnosis_safe_' ~ blockchain, source_info) }}
    {%- if not loop.last %}
    
    union 
    {% endif %}
    {%- endfor %}
    {%- endif %}
)
SELECT DISTINCT address
FROM all_singletons
WHERE LOWER(address) IN (
    {%- for addr in official_addresses %}
    LOWER('{{ addr }}'){% if not loop.last %},{% endif %}
    {%- endfor %}
)
{% endmacro %}

{% macro safe_singletons_ethereum_validated() %}
-- Fetch all known singleton/mastercopy addresses used via factories for Ethereum
-- FILTERED to only include official Safe deployments
{%- set official_addresses = get_official_safe_addresses() -%}
WITH all_singletons AS (
    select distinct masterCopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_0_0_call_createProxy') }}
    
    union 
    
    select distinct _mastercopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_0_0_call_createProxyWithNonce') }}
    
    union
    
    select distinct masterCopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_0_call_createProxy') }}
    
    union 
    select distinct _mastercopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_0_call_createProxyWithNonce') }}
    
    union
    
    select distinct masterCopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxy') }}
    
    union 
    
    select distinct _mastercopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxyWithNonce') }}
    
    union
    
    select distinct _mastercopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxyWithCallback') }}
    
    union
    
    select distinct singleton as address 
    from {{ source('gnosis_safe_ethereum', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}
    
    union
    
    select distinct singleton as address
    from {{ source('gnosis_safe_ethereum', 'SafeProxyFactory_v_1_4_1_evt_ProxyCreation') }}
)
SELECT DISTINCT address
FROM all_singletons
WHERE LOWER(address) IN (
    {%- for addr in official_addresses %}
    LOWER('{{ addr }}'){% if not loop.last %},{% endif %}
    {%- endfor %}
)
{% endmacro %}

{% macro safe_singletons_by_network_validated(blockchain, only_official=true) %}
{#- 
Main macro to get singletons for a network
Set only_official=true to filter for official Safe deployments only
Set only_official=false to get all discovered singletons (including unofficial)
#}
{%- set network_config = get_safe_network_config(blockchain) -%}

{%- if only_official -%}
    {%- if network_config.singleton_type == 'modern' -%}
        {{ safe_singletons_modern_validated(blockchain, network_config.singleton_sources) }}
    {%- elif network_config.singleton_type == 'legacy' -%}
        {{ safe_singletons_legacy_validated(blockchain, network_config.legacy_singleton_sources, network_config.modern_singleton_sources) }}
    {%- elif network_config.singleton_type == 'legacy_ethereum' -%}
        {{ safe_singletons_ethereum_validated() }}
    {%- else -%}
        {{ exceptions.raise_compiler_error("Unknown singleton type: " ~ network_config.singleton_type) }}
    {%- endif -%}
{%- else -%}
    -- Return all discovered singletons without filtering
    {%- if network_config.singleton_type == 'modern' -%}
        {{ safe_singletons_modern(blockchain, network_config.singleton_sources) }}
    {%- elif network_config.singleton_type == 'legacy' -%}
        {{ safe_singletons_legacy(blockchain, network_config.legacy_singleton_sources, network_config.modern_singleton_sources) }}
    {%- elif network_config.singleton_type == 'legacy_ethereum' -%}
        {{ safe_singletons_ethereum() }}
    {%- else -%}
        {{ exceptions.raise_compiler_error("Unknown singleton type: " ~ network_config.singleton_type) }}
    {%- endif -%}
{%- endif -%}
{% endmacro %}