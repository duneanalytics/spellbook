{{
    config(
        materialized='table',
        schema = 'safe_ronin',
        alias= 'singletons'
        , post_hook='{{ hide_spells() }}'
    )
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
select distinct singleton as address
from {{ source('safe_ronin', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}
