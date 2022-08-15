{{ 
    config(
        materialized='incremental',
        alias='safes',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge'
    ) 
}}

select
    et.from as address,
    case 
        when et.to = '0x8942595a2dc5181df0465af0d7be08c8f23c93af' then '0.1.0'
        when et.to = '0xb6029ea3b2c51d09a50b53ca8012feeb05bda35a' then '1.0.0'
        when et.to = '0xae32496491b53841efb51829d6f886387708f99b' then '1.1.0'
        when et.to = '0x34cfac646f301356faa8b21e94227e3583fe3f5f' then '1.1.1'
        when et.to = '0x6851d6fdfafd08c0295c392436245e5bc78b0185' then '1.2.0'
    end as creation_version,
    try_cast(date_trunc('day', et.block_time) as date) as block_date,
    et.block_time as creation_time,
    et.tx_hash
from {{ source('ethereum', 'traces') }} et 
where et.success = true
    and et.block_time > '2018-11-24' -- for initial query optimisation    
    and et.call_type = 'delegatecall' -- the delegate call to the master copy is the Safe address    
    and (
            (substring(et.input, 0, 10) = '0x0ec78d9e' -- setup methods of v0.1.0
                and et.to = '0x8942595a2dc5181df0465af0d7be08c8f23c93af') -- mastercopy v0.1.0
            or
            (substring(et.input, 0, 10) = '0xa97ab18a' -- setup methods of v1.0.0
                and et.to = '0xb6029ea3b2c51d09a50b53ca8012feeb05bda35a') -- mastercopy v1.0.0
            or
            (substring(et.input, 0, 10) = '0xb63e800d' -- setup methods of v1.1.0, v1.1.1, 1.2.0
                and et.to in (
                    '0xae32496491b53841efb51829d6f886387708f99b',  -- mastercopy v1.1.0
                    '0x34cfac646f301356faa8b21e94227e3583fe3f5f',  -- mastercopy v1.1.1
                    '0x6851d6fdfafd08c0295c392436245e5bc78b0185')  -- mastercopy v1.2.0
                )
        )
    and et.gas_used > 0  -- to ensure the setup call was successful
    {% if is_incremental() %}
    and et.block_time > (select max(creation_time) from {{ this }}) - interval '1 day'
    {% endif %}
        
union all
    
select contract_address as address, 
    '1.3.0' as creation_version, 
    try_cast(date_trunc('day', evt_block_time) as date) as block_date,
    evt_block_time as creation_time, 
    evt_tx_hash as tx_hash
from {{ source('gnosis_safe_ethereum', 'GnosisSafev1_3_0_evt_SafeSetup') }}
where evt_block_time > '2018-11-24' -- for initial query optimisation  
{% if is_incremental() %}
and evt_block_time > (select max(creation_time) from {{ this }}) - interval '1 day'
{% endif %}