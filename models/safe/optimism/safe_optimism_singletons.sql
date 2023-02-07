{{ 
    config(
        materialized='incremental',
        alias='singletons',
        unique_key = ['address'],
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz"]\') }}'
    ) 
}}


-- Fetch all known singleton addresses used via the factory.
select distinct singleton as address 
from {{ source('gnosis_safe_optimism', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}
{% if is_incremental() %} 
where evt_block_time >= date_trunc('day', now() - interval '1 week') 
{% endif %}
