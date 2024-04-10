{{
    config(
        materialized='table',
        
        schema='safe_celo',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida", "peterrliem"]\') }}'
    )
}}


-- Fetch all known singleton addresses used via the factory.
select distinct singleton as address
from {{ source('gnosis_safe_celo', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}

union 
select distinct singleton as address
from {{ source('gnosis_safe_celo', 'SafeProxyFactory_v_1_4_1_evt_ProxyCreation') }}
