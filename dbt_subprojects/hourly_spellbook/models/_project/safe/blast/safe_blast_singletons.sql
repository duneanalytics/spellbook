{{
    config(
        materialized='table',
        schema = 'safe_blast',
        alias= 'singletons',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
    )
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
select distinct singleton as address
from {{ source('gnosis_safe_blast', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}

union

select distinct singleton as address
from {{ source('gnosis_safe_blast', 'SafeProxyFactory_v1_4_1_evt_ProxyCreation') }}
