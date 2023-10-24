{{ 
    config(
        materialized='table',
        alias = 'singletons',
        
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    ) 
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
-- Prior to 1.3.0, the factory didn't emit the singleton address with the ProxyCreation event,
select distinct masterCopy as address 
from {{ source('gnosis_safe_gnosis', 'ProxyFactory_v1_1_1_call_createProxy') }}

union 

select distinct _mastercopy as address 
from {{ source('gnosis_safe_gnosis', 'ProxyFactory_v1_1_1_call_createProxyWithNonce') }}

union

select distinct _mastercopy as address 
from {{ source('gnosis_safe_gnosis', 'ProxyFactory_v1_1_1_call_createProxyWithCallback') }}

union

select distinct singleton as address 
from {{ source('gnosis_safe_gnosis', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}

union

-- The Circles project used a custom Safe master copy, not via the official factories though, adding that manually.
select 0x2cb0ebc503de87cfd8f0eceed8197bf7850184ae as address