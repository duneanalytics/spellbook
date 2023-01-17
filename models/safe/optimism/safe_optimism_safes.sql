{{ 
    config(
        materialized='incremental',
        alias='safes',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "safe",
                                    \'["frank_maseo"]\') }}'
    ) 
}}

select 
    'optimism' as blockchain,
    contract_address as address,
    '1.3.0' as creation_version, 
    try_cast(date_trunc('day', evt_block_time) as date) as block_date,
    evt_block_time as creation_time,
    evt_tx_hash as tx_hash
from {{ source('gnosis_safe_optimism', 'GnosisSafeL2_v1_3_0_evt_SafeSetup') }}
{% if not is_incremental() %}
where evt_block_time > '2021-11-17' -- for initial query optimisation 
{% endif %} 
{% if is_incremental() %}
where evt_block_time > date_trunc("day", now() - interval '1 week')
{% endif %}