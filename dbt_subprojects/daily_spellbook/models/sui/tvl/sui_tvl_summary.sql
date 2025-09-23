{{ config(
    schema='sui_tvl',
    alias='summary',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['protocol', 'object_type', 'block_date'],
    partition_by=['block_date'],
    tags=['sui','tvl','summary']
) }}

-- Daily TVL summary by protocol and object type
-- Provides aggregated view of total value locked

select
    protocol,
    object_type,
    block_date,
    count(distinct market_id) as pool_count,
    count(distinct coin_type) as unique_tokens,
    sum(tvl_native_amount) as total_native_amount,
    sum(tvl_usd) as total_tvl_usd,
    avg(protocol_fee_rate) as avg_fee_rate,
    max(block_date) as latest_update
from {{ ref('sui_tvl_gold') }}
{% if is_incremental() %}
where {{ incremental_predicate('block_date') }}
{% endif %}
group by 
    protocol,
    object_type,
    block_date 