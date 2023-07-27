{{ 
    config(
	tags=['legacy'],
	
        materialized='table',
        alias = alias('singletons', legacy_model=True),
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    ) 
}}


-- Fetch all known singleton addresses used via the factory.
select distinct singleton as address 
from {{ source('gnosis_safe_avalanche_c', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}