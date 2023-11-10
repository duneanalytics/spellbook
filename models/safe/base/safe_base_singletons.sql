{{
    config(
        materialized='table',
        
        alias= 'singletons',
        post_hook='{{ expose_spells(\'["base"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    )
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
select distinct singleton as address
from {{ source('gnosis_safe_base', 'GnosisSafeProxyFactoryv_1_3_0_evt_ProxyCreation') }}