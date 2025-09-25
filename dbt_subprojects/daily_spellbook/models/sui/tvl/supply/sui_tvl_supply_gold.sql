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

-- Gold layer: BTC supply with USD pricing and business metrics
-- Following the Snowflake BTC_SUPPLY_GOLD pattern exactly
-- Adds BTC pricing to enable downstream ecosystem analysis

select
    s.date as block_date,
    s.total_btc_supply,
    s.supply_breakdown_json,
    
    -- Add BTC pricing for USD valuation
    p.price as btc_price_usd,
    cast(coalesce(cast(s.total_btc_supply as double) * p.price, 0) as decimal(38,8)) as total_btc_usd_value
    
from {{ ref('sui_tvl_supply_silver') }} s

-- Join BTC pricing (using standard wrapped BTC token for pricing)
left join {{ source('prices','usd') }} p
    on p.blockchain = 'sui'
    and date(p.minute) = s.date
    and p.contract_address = cast(
        regexp_replace(
            split_part(
                lower('0x27792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN'), 
                '::', 1
            ),
            '^0x0*([0-9a-f]+)$', '0x$1'
        ) as varbinary
    )

{% if is_incremental() %}
where {{ incremental_predicate('s.date') }}
{% endif %}

order by block_date desc 