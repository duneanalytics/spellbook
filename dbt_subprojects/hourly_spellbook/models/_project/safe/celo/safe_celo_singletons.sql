{{
    config(
        materialized='table',
        
        schema='safe_celo',
        alias = 'singletons'
        , post_hook='{{ hide_spells() }}'
    )
}}


-- Fetch all known singleton addresses used via the factory.
select distinct singleton as address
from {{ source('gnosis_safe_celo', 'safeproxyfactory_v1_3_0_evt_proxycreation') }}

union

select distinct singleton as address
from {{ source('gnosis_safe_celo', 'SafeProxyFactory_v1_4_1_evt_ProxyCreation') }}

union

select distinct singleton as address
from {{ source('safe_celo', 'safeproxyfactory_evt_proxycreation') }} --1.5.0