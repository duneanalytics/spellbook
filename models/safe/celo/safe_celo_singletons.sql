{{
    config(
        materialized='table',
        tags = ['dunesql'],
        schema='safe_celo',
        alias = alias('singletons'),
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    )
}}


-- Fetch all known singleton addresses used via the factory.
select distinct singleton as address
from {{ source('gnosis_safe_celo', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}