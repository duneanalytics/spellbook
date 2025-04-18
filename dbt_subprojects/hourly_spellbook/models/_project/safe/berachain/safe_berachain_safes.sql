{{
    config(
        materialized='incremental',
        schema = 'safe_berachain',
        alias= 'safes',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook = '{{ expose_spells(
                        blockchains = \'["berachain"]\',
                        spell_type = "project",
                        spell_name = "safe",
                        contributors = \'["petertherock"]\') }}'
    )
}}

{% set project_start_date = '2025-01-01' %}

select
    'berachain' as blockchain,
    et."from" as address,
    case
        when et.to = 0xd9db270c1b5e3bd161e8c8503c55ceabee709552 then '1.3.0'
        when et.to = 0x3e5c63644e683549055b9be8653de26e0b4cd36e then '1.3.0L2'
        when et.to = 0x41675C099F32341bf84BFc5382aF534df5C7461a then '1.4.1'
        when et.to = 0x29fcB43b46531BcA003ddC8FCB67FFE91900C762 then '1.4.1L2'
        else 'unknown'
    end as creation_version,
    try_cast(date_trunc('day', et.block_time) as date) as block_date,
    CAST(date_trunc('month', et.block_time) as DATE) as block_month,
    et.block_time as creation_time,
    et.tx_hash
from {{ source('berachain', 'traces') }} et
join {{ ref('safe_berachain_singletons') }} s
    on et.to = s.address
where et.success = true
    and et.call_type = 'delegatecall' -- delegatecall to singleton is Safe (proxy) address
    and bytearray_substring(et.input, 1, 4) in (
        0xb63e800d -- setup method v1.3.0, v1.3.0L2, v1.4.1, v.1.4.1L2
    )
    and et.gas_used > 10000  -- to ensure the setup call was successful. excludes e.g. setup calls with missing params that fallback
    {% if not is_incremental() %}
    and et.block_time > TIMESTAMP '{{project_start_date}}' -- for initial query optimisation
    {% else %}
    and {{ incremental_predicate('et.block_time') }}
    {% endif %}
