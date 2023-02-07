{{ 
    config(
        materialized='incremental',
        alias='singletons',
        unique_key = ['address'],
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    ) 
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
-- Prior to 1.3.0, the factory didn't emit the singleton address with the ProxyCreation event,
select distinct masterCopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_0_0_call_createProxy') }}
{% if is_incremental() %} 
where call_block_time >= date_trunc('day', now() - interval '1 week') 
{% endif %}

union 

select distinct _mastercopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_0_0_call_createProxyWithNonce') }}
{% if is_incremental() %} 
where call_block_time >= date_trunc('day', now() - interval '1 week') 
{% endif %}

union

select distinct masterCopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_0_call_createProxy') }}
{% if is_incremental() %} 
where call_block_time >= date_trunc('day', now() - interval '1 week') 
{% endif %}

union 
select distinct _mastercopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_0_call_createProxyWithNonce') }}
{% if is_incremental() %} 
where call_block_time >= date_trunc('day', now() - interval '1 week') 
{% endif %}

union

select distinct masterCopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxy') }}
{% if is_incremental() %} 
where call_block_time >= date_trunc('day', now() - interval '1 week') 
{% endif %}

union 

select distinct _mastercopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxyWithNonce') }}
{% if is_incremental() %} 
where call_block_time >= date_trunc('day', now() - interval '1 week') 
{% endif %}

union

select distinct _mastercopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxyWithCallback') }}
{% if is_incremental() %} 
where call_block_time >= date_trunc('day', now() - interval '1 week') 
{% endif %}

union

select distinct singleton as address 
from {{ source('gnosis_safe_ethereum', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}
{% if is_incremental() %} 
where evt_block_time >= date_trunc('day', now() - interval '1 week') 
{% endif %}
