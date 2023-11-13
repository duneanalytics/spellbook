{{
    config(
        materialized='incremental',
        schema = 'safe_zksync',
        alias = 'safes',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["zksync"]\',
                                    "project",
                                    "safe",
                                    \'["danielpartida", "kryptaki"]\') }}'
    )
}}

select
    'zksync' as blockchain,
    proxy as address,
    case
        when singleton = 0xB00ce5CCcdEf57e539ddcEd01DF43a13855d9910 then '1.3.0'
        when singleton = 0x1727c2c531cf966f902E5927b98490fDFb3b2b70 then '1.3.0L2'
        else 'unknown'
    end as creation_version,
    try_cast(date_trunc('day', evt_block_time) as date) as block_date,
    CAST(date_trunc('month', evt_block_time) as DATE) as block_month,
    evt_block_time as creation_time,
    evt_tx_hash as tx_hash
from {{ source('gnosis_safe_zksync', 'GnosisSafeProxyFactoryv1_3_0_evt_ProxyCreation') }}
where 
    {% if not is_incremental() %}
    evt_block_time > TIMESTAMP '2023-09-01' -- for initial query optimisation
    {% endif %}
    {% if is_incremental() %}
    {{ incremental_predicate('evt_block_time') }}
    {% endif %}
