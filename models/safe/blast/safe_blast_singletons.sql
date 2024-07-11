{{
    config(
        materialized='table',

        alias= 'singletons',
        post_hook='{{ expose_spells(\'["blast"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    )
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
select distinct singleton as address
from {{ source('gnosis_safe_blast', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}

union

select distinct singleton as address
from {{ source('gnosis_safe_blast', 'SafeProxyFactory_v1_4_1_evt_ProxyCreation') }}
