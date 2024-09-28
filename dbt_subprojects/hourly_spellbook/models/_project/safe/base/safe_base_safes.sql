{{
    config(
        materialized='incremental',
        
        alias= 'safes',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["base"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida"]\') }}'
    )
}}

select
    'base' as blockchain,
    et."from" as address,
    case
        when et.to = 0xd9db270c1b5e3bd161e8c8503c55ceabee709552 then '1.3.0'
        when et.to = 0x69f4d1788e39c87893c980c06edf4b7f686e2938 then '1.3.0'  -- for chains with EIP-155
        when et.to = 0x3e5c63644e683549055b9be8653de26e0b4cd36e then '1.3.0L2'
        when et.to = 0xfb1bffc9d739b8d520daf37df666da4c687191ea then '1.3.0L2' -- for chains with EIP-155
        when et.to = 0x41675C099F32341bf84BFc5382aF534df5C7461a then '1.4.1'
        when et.to = 0x29fcB43b46531BcA003ddC8FCB67FFE91900C762 then '1.4.1L2'
        else 'unknown'
    end as creation_version,
    try_cast(date_trunc('day', et.block_time) as date) as block_date,
    CAST(date_trunc('month', et.block_time) as DATE) as block_month,
    et.block_time as creation_time,
    et.tx_hash
from {{ source('base', 'traces') }} et
join {{ ref('safe_base_singletons') }} s
    on et.to = s.address
where et.success = true
    and et.call_type = 'delegatecall' -- delegatecall to singleton is Safe (proxy) address
    and bytearray_substring(et.input, 1, 4) in (
        0x0ec78d9e, -- setup method v0.1.0
        0xa97ab18a, -- setup method v1.0.0
        0xb63e800d -- setup method v1.1.0, v1.1.1, v1.2.0, v1.3.0, v1.3.0L2, v1.4.1, v.1.4.1L2
    )
    and et.gas_used > 10000  -- to ensure the setup call was successful. excludes e.g. setup calls with missing params that fallback
    {% if not is_incremental() %}
    and et.block_time > TIMESTAMP '2023-07-01' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    and et.block_time > date_trunc('day', now() - interval '7' day)
    {% endif %}
