{{ 
    config(
        materialized='table',
        alias = 'singletons'
        , post_hook='{{ hide_spells() }}'
    ) 
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
-- Prior to 1.3.0, the factory didn't emit the singleton address with the ProxyCreation event,



select distinct singleton as address
from {{ source('gnosis_safe_hyperevm', 'SafeProxyFactory_v1_4_1_evt_ProxyCreation') }}

union

select distinct singleton as address
from {{ source('gnosis_safe_hyperevm', 'SafeProxyFactory_v1_5_0_evt_ProxyCreation') }}