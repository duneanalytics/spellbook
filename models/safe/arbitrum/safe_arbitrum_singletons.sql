{{ 
    config(
        materialized='table',
        tags = ['dunesql'],
        alias = alias('singletons'),
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    ) 
}}


-- Fetch all known singleton addresses used via the factory.
select distinct singleton as address 
from {{ source('gnosis_safe_arbitrum', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}