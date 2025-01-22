{{
    config(
        materialized='table',
        schema = 'safe_zksync',
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["zksync"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida", "kryptaki"]\') }}'
    )
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
select distinct singleton as address
from {{ source('gnosis_safe_zksync', 'GnosisSafeProxyFactoryv1_3_0_evt_ProxyCreation') }}