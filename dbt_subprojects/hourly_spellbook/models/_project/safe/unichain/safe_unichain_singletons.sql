{{ 
    config(
        materialized='table',
        
        alias = 'singletons',
        post_hook='{{ expose_spells(\'["unichain"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz", "peterrliem", "safehjc"]\') }}'
    ) 
}}


-- Fetch all known singleton addresses used via the factory.
select distinct singleton as address 
from {{ source('gnosis_safe_unichain', 'safeproxyfactory_v1_3_0_evt_proxycreation') }}

union 

select distinct singleton as address 
from {{ source('gnosis_safe_unichain', 'safeproxyfactory_v1_4_1_evt_proxycreation') }}
