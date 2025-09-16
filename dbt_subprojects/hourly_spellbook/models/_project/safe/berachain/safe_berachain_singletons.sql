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
from {{ source('safe_berachain', 'safeproxyfactory_v1_3_0_evt_proxycreation') }}

union
select distinct singleton as address
from {{ source('safe_berachain', 'safeproxyfactory_v1_4_1_evt_proxycreation') }}
