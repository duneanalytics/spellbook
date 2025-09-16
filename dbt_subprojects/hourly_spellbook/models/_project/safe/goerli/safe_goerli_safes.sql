{{ 
    config(
        materialized='incremental',
        
        alias = 'safes',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "project",
                                    "safe",
                                    \'["tschubotz", "hosuke", "danielpartida"]\') }}'
    ) 
}}

select
    'goerli' as blockchain,
    et."from" as address,
    case 
        when et.to = 0x8942595a2dc5181df0465af0d7be08c8f23c93af then '0.1.0'
        when et.to = 0xb6029ea3b2c51d09a50b53ca8012feeb05bda35a then '1.0.0'
        when et.to = 0xae32496491b53841efb51829d6f886387708f99b then '1.1.0'
        when et.to = 0x34cfac646f301356faa8b21e94227e3583fe3f5f then '1.1.1'
        when et.to = 0x6851d6fdfafd08c0295c392436245e5bc78b0185 then '1.2.0'
        when et.to = 0xd9db270c1b5e3bd161e8c8503c55ceabee709552 then '1.3.0'
        when et.to = 0x69f4d1788e39c87893c980c06edf4b7f686e2938 then '1.3.0'  -- for chains with EIP-155
        when et.to = 0x3e5c63644e683549055b9be8653de26e0b4cd36e then '1.3.0L2'
        when et.to = 0xfb1bffc9d739b8d520daf37df666da4c687191ea then '1.3.0L2' -- for chains with EIP-155
        when et.to = 0x41675c099f32341bf84bfc5382af534df5c7461a then '1.4.1'
        when et.to = 0x29fcb43b46531bca003ddc8fcb67ffe91900c762 then '1.4.1L2'
        else 'unknown'
    end as creation_version,
    try_cast(date_trunc('day', et.block_time) as date) as block_date,
    CAST(date_trunc('month', et.block_time) as DATE) as block_month,
    et.block_time as creation_time,
    et.tx_hash
from {{ source('goerli', 'traces') }} et 
join {{ ref('safe_goerli_singletons') }} s
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
    and et.block_time > TIMESTAMP '2019-09-03' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    and et.block_time > date_trunc('day', now() - interval '7' day)
    {% endif %}
