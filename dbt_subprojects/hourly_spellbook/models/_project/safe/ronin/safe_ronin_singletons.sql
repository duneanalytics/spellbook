{{
    config(
        materialized='table',
        schema = 'safe_ronin',
        alias= 'singletons',
        post_hook = '{{ expose_spells(
                        blockchains = \'["ronin"]\',
                        spell_type = "project",
                        spell_name = "safe",
                        contributors = \'["petertherock"]\') }}'
    )
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
select distinct singleton as address
from {{ source('safe_ronin', 'SafeProxyFactory_v_1_3_0_evt_ProxyCreation') }}
