{{ 
    config(
        materialized='table',
        
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    ) 
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
-- Prior to 1.3.0, the factory didn't emit the singleton address with the ProxyCreation event,
select distinct masterCopy as address 
from {{ source('gnosis_safe_bnb', 'ProxyFactory_v1_1_1_call_createProxy') }}

union 

select distinct _mastercopy as address 
from {{ source('gnosis_safe_bnb', 'ProxyFactory_v1_1_1_call_createProxyWithNonce') }}

union

select distinct _mastercopy as address 
from {{ source('gnosis_safe_bnb', 'ProxyFactory_v1_1_1_call_createProxyWithCallback') }}

union

select distinct singleton as address 
from {{ source('gnosis_safe_bnb', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}