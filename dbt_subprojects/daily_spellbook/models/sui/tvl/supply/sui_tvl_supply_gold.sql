{{ config(
    schema='sui_tvl',
    alias='supply_gold',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date'],
    partition_by=['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','supply','gold']
) }}

-- Gold layer: Simple token supply with USD valuation (following Snowflake pattern)
-- Basic pricing without complex metrics

select
    s.block_date,
    s.total_token_supply,
    s.token_count,
    
    -- Get SUI price for basic USD valuation
    p.price as sui_price_usd,
    cast(coalesce(cast(s.total_token_supply as double) * p.price, 0) as decimal(38,8)) as estimated_total_supply_usd
    
from {{ ref('sui_tvl_supply_silver') }} s

-- Join SUI pricing for basic USD estimation
left join {{ source('prices','usd') }} p
    on p.blockchain = 'sui'
    and date(p.minute) = s.block_date
    and p.contract_address = cast('0x0000000000000000000000000000000000000000000000000000000000000002' as varbinary)

{% if is_incremental() %}
where {{ incremental_predicate('s.block_date') }}
{% endif %}

order by block_date desc 