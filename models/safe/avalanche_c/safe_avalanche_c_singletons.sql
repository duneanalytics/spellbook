{{ 
    config(
        materialized='table',
        
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz", "peterrliem"]\') }}'
    ) 
}}


-- Fetch all known singleton addresses used via the factory.
select distinct singleton as address 
from {{ source('gnosis_safe_avalanche_c', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}

union

select distinct singleton as address
from {{ source('gnosis_safe_avalanche_c', 'SafeProxyFactory_v_1_4_1_evt_ProxyCreation') }}
