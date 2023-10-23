{{ 
    config(
        materialized='table',
        
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    ) 
}}


-- Fetch all known singleton addresses used via the factory.
select distinct singleton as address 
from {{ source('gnosis_safe_fantom', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}