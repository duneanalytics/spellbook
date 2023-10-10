{{
    config(
        materialized='table',
        tags = ['dunesql'],
        alias= alias('singletons'),
        post_hook='{{ expose_spells(\'["zksync"]\',
                                    "project",
                                    "safe",
                                    \'["kryptaki"]\') }}'
    )
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
select distinct singleton as address
from {{ source('gnosis_safe_zksync', 'GnosisSafeProxyFactoryv_1_3_0_evt_ProxyCreation') }}