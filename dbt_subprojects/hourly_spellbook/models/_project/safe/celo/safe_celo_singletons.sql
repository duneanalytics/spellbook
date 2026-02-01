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
from {{ source('gnosis_safe_celo', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}