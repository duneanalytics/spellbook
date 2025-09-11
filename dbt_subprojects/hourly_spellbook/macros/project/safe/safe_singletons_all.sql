{% macro get_official_safe_deployments() %}
    {#- 
    Single source of truth for all official Safe singleton addresses and their versions
    Returns a dictionary mapping addresses to their version info
    
    Official Safe Singleton Addresses by version:
    ============================================
    v0.1.0 - Safe:   0x8942595A2dC5181Df0465af0D7be08c8f23C93af (pre-audit, not validated in singletons)
    v1.0.0 - Safe:   0xb6029EA3B2c51D09a50B53CA8012FEeB05bDa35A
    v1.1.0 - Safe:   0xae32496491b53841efb51829d6f886387708F99B (not validated in singletons)
    v1.1.1 - Safe:   0x34CfAC646f301356fAa8B21e94227e3583Fe3F5F
    v1.2.0 - Safe:   0x6851D6fDFAfD08c0295C392436245E5bc78B0185
    v1.3.0 - Safe:   0xd9Db270c1B5E3Bd161E8c8503c55cEABeE709552
    v1.3.0 - Safe:   0x69f4D1788e39c87893C980c06EdF4b7f686e2938 (EIP-155)
    v1.3.0 - Safe:   0xB00ce5CCcdEf57e539ddcEd01DF43a13855d9910 (ZKSync)
    v1.3.0 - SafeL2: 0x3E5c63644E683549055b9Be8653de26E0B4CD36E
    v1.3.0 - SafeL2: 0xfb1bffC9d739B8D520DaF37dF666da4C687191EA (EIP-155)
    v1.3.0 - SafeL2: 0x1727c2c531cf966f902E5927b98490fDFb3b2b70 (ZKSync)
    v1.4.1 - Safe:   0x41675C099F32341bf84BFc5382aF534df5C7461a
    v1.4.1 - Safe:   0xC35F063962328aC65cED5D4c3fC5dEf8dec68dFa (ZKSync)
    v1.4.1 - SafeL2: 0x29fcB43b46531BcA003ddC8FCB67FFE91900C762
    v1.4.1 - SafeL2: 0x610fcA2e0279Fa1F8C00c8c2F71dF522AD469380 (ZKSync)
    v1.5.0 - Safe:   0xFf51A5898e281Db6DfC7855790607438dF2ca44b
    v1.5.0 - SafeL2: 0xEdd160fEBBD92E350D4D398fb636302fccd67C7e
    
    Source: https://github.com/safe-global/safe-deployments
    #}
    {%- set deployments = {
        '0x8942595A2dC5181Df0465af0D7be08c8f23C93af': {'version': '0.1.0', 'validate': false},
        '0xb6029EA3B2c51D09a50B53CA8012FEeB05bDa35A': {'version': '1.0.0', 'validate': true},
        '0xae32496491b53841efb51829d6f886387708F99B': {'version': '1.1.0', 'validate': false},
        '0x34CfAC646f301356fAa8B21e94227e3583Fe3F5F': {'version': '1.1.1', 'validate': true},
        '0x6851D6fDFAfD08c0295C392436245E5bc78B0185': {'version': '1.2.0', 'validate': true},
        '0xd9Db270c1B5E3Bd161E8c8503c55cEABeE709552': {'version': '1.3.0', 'validate': true},
        '0x69f4D1788e39c87893C980c06EdF4b7f686e2938': {'version': '1.3.0', 'validate': true, 'note': 'EIP-155'},
        '0xB00ce5CCcdEf57e539ddcEd01DF43a13855d9910': {'version': '1.3.0', 'validate': true, 'note': 'ZKSync'},
        '0x3E5c63644E683549055b9Be8653de26E0B4CD36E': {'version': '1.3.0L2', 'validate': true},
        '0xfb1bffC9d739B8D520DaF37dF666da4C687191EA': {'version': '1.3.0L2', 'validate': true, 'note': 'EIP-155'},
        '0x1727c2c531cf966f902E5927b98490fDFb3b2b70': {'version': '1.3.0L2', 'validate': true, 'note': 'ZKSync'},
        '0x41675C099F32341bf84BFc5382aF534df5C7461a': {'version': '1.4.1', 'validate': true},
        '0xC35F063962328aC65cED5D4c3fC5dEf8dec68dFa': {'version': '1.4.1', 'validate': true, 'note': 'ZKSync'},
        '0x29fcB43b46531BcA003ddC8FCB67FFE91900C762': {'version': '1.4.1L2', 'validate': true},
        '0x610fcA2e0279Fa1F8C00c8c2F71dF522AD469380': {'version': '1.4.1L2', 'validate': true, 'note': 'ZKSync'},
        '0xFf51A5898e281Db6DfC7855790607438dF2ca44b': {'version': '1.5.0', 'validate': true},
        '0xEdd160fEBBD92E350D4D398fb636302fccd67C7e': {'version': '1.5.0L2', 'validate': true}
    } -%}
    {{ return(deployments) }}
{% endmacro %}

{% macro get_official_safe_addresses() %}
    {#- 
    Returns a list of official Safe singleton addresses that should be validated
    Filters the full deployment list to only include addresses marked for validation
    #}
    {%- set deployments = get_official_safe_deployments() -%}
    {%- set addresses = [] -%}
    {%- for address, info in deployments.items() -%}
        {%- if info.validate -%}
            {%- set _ = addresses.append(address) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(addresses) }}
{% endmacro %}

{% macro safe_singletons_modern_validated(blockchain, sources_list) %}
-- Fetch all known singleton addresses used via the factory
-- FILTERED to only include official Safe deployments
{% set official_addresses = get_official_safe_addresses() %}
{% if sources_list and sources_list|length > 0 %}

WITH all_singletons AS (
    {%- for source_info in sources_list %}
    select distinct singleton as address 
    from {{ source('gnosis_safe_' ~ blockchain, source_info) }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
    {%- if not loop.last %}
    
    union 
    {% endif %}
    {%- endfor %}
)
SELECT DISTINCT address
FROM all_singletons
WHERE LOWER(CAST(address AS VARCHAR)) IN (
    {%- for addr in official_addresses %}
    LOWER('{{ addr }}'){% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- else -%}
-- No source tables available for this network yet
SELECT CAST(NULL AS VARCHAR) AS address
WHERE FALSE
{%- endif -%}
{% endmacro %}

{% macro safe_singletons_legacy_validated(blockchain, legacy_sources, modern_sources) %}
-- Fetch all known singleton/mastercopy addresses used via factories
-- FILTERED to only include official Safe deployments
{% set official_addresses = get_official_safe_addresses() %}
{% set has_sources = (legacy_sources and legacy_sources|length > 0) or (modern_sources and modern_sources|length > 0) %}
{% if has_sources %}

WITH all_singletons AS (
    {%- for source_info in legacy_sources %}
    select distinct {{ source_info.column }} as address 
    from {{ source('gnosis_safe_' ~ blockchain, source_info.table) }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('call_block_time') }}
    {%- endif %}
    {%- if not loop.last or modern_sources|length > 0 %}
    
    union 
    {% endif %}
    {%- endfor %}
    
    {%- if modern_sources|length > 0 %}
    {%- for source_info in modern_sources %}
    select distinct singleton as address 
    from {{ source('gnosis_safe_' ~ blockchain, source_info) }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
    {%- if not loop.last %}
    
    union 
    {% endif %}
    {%- endfor %}
    {%- endif %}
)
SELECT DISTINCT address
FROM all_singletons
WHERE LOWER(CAST(address AS VARCHAR)) IN (
    {%- for addr in official_addresses %}
    LOWER('{{ addr }}'){% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- else -%}
-- No source tables available for this network yet
SELECT CAST(NULL AS VARCHAR) AS address
WHERE FALSE
{%- endif -%}
{% endmacro %}

{% macro safe_singletons_ethereum_validated() %}
-- Fetch all known singleton/mastercopy addresses used via factories for Ethereum
-- FILTERED to only include official Safe deployments
{% set official_addresses = get_official_safe_addresses() %}

WITH all_singletons AS (
    select distinct masterCopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_0_0_call_createProxy') }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('call_block_time') }}
    {%- endif %}
    
    union 
    
    select distinct _mastercopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_0_0_call_createProxyWithNonce') }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('call_block_time') }}
    {%- endif %}
    
    union
    
    select distinct masterCopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_0_call_createProxy') }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('call_block_time') }}
    {%- endif %}
    
    union 
    select distinct _mastercopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_0_call_createProxyWithNonce') }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('call_block_time') }}
    {%- endif %}
    
    union
    
    select distinct masterCopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxy') }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('call_block_time') }}
    {%- endif %}
    
    union 
    
    select distinct _mastercopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxyWithNonce') }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('call_block_time') }}
    {%- endif %}
    
    union
    
    select distinct _mastercopy as address 
    from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxyWithCallback') }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('call_block_time') }}
    {%- endif %}
    
    union
    
    select distinct singleton as address 
    from {{ source('gnosis_safe_ethereum', 'SafeProxyFactory_v1_3_0_evt_ProxyCreation') }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
    
    union
    
    select distinct singleton as address
    from {{ source('gnosis_safe_ethereum', 'SafeProxyFactory_v1_4_1_evt_ProxyCreation') }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
    
    union
    
    select distinct singleton as address
    from {{ source('gnosis_safe_ethereum', 'SafeProxyFactory_v1_5_0_evt_ProxyCreation') }}
    {%- if not is_incremental() %}
    -- no filter for initial load
    {%- else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
)
SELECT DISTINCT address
FROM all_singletons
WHERE LOWER(CAST(address AS VARCHAR)) IN (
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
