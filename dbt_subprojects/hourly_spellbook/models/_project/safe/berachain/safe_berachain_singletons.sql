{{
    config(
        materialized='table',
        schema = 'safe_berachain',
        alias= 'singletons',
        post_hook = '{{ expose_spells(
                        blockchains = \'["berachain"]\',
                        spell_type = "project",
                        spell_name = "safe",
                        contributors = \'["petertherock"]\') }}'
    )
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
select distinct singleton as address
from {{ source('safe_berachain', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}

union
select distinct singleton as address
from {{ source('safe_berachain', 'SafeProxyFactory_v_1_4_1_evt_ProxyCreation') }}
