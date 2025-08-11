{% macro safe_singletons_modern(blockchain, sources_list) %}
-- Fetch all known singleton addresses used via the factory.
{%- for source_info in sources_list %}
select distinct singleton as address 
from {{ source('gnosis_safe_' ~ blockchain, source_info) }}
{%- if not loop.last %}

union 
{% endif %}
{%- endfor %}
{% endmacro %}

{% macro safe_singletons_legacy(blockchain, legacy_sources, modern_sources) %}
-- Fetch all known singleton/mastercopy addresses used via factories.
-- Prior to 1.3.0, the factory didn't emit the singleton address with the ProxyCreation event,
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
{% endmacro %}

{% macro safe_singletons_by_network(blockchain) %}
{%- set network_config = get_safe_network_config(blockchain) -%}

{%- if network_config.singleton_type == 'modern' -%}
    {{ safe_singletons_modern(blockchain, network_config.singleton_sources) }}
{%- elif network_config.singleton_type == 'legacy' -%}
    {{ safe_singletons_legacy(blockchain, network_config.legacy_singleton_sources, network_config.modern_singleton_sources) }}
{%- elif network_config.singleton_type == 'legacy_ethereum' -%}
    {{ safe_singletons_ethereum() }}
{%- else -%}
    {{ exceptions.raise_compiler_error("Unknown singleton type: " ~ network_config.singleton_type) }}
{%- endif -%}
{% endmacro %}

{% macro safe_singletons_ethereum() %}
-- Fetch all known singleton/mastercopy addresses used via factories.
-- Prior to 1.3.0, the factory didn't emit the singleton address with the ProxyCreation event,
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
{% endmacro %}
