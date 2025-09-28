{{ config(
    schema='sui_tvl'
    , alias='supply_gold'
    , materialized='incremental'
    , file_format='delta'
    , incremental_strategy='merge'
    , unique_key=['block_date']
    , partition_by=['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , tags=['sui','tvl','supply','gold']
) }}

-- Gold layer: BTC supply with USD pricing and business metrics
-- Adds BTC pricing to enable downstream ecosystem analysis

select
    s.block_date
    , s.total_btc_supply
    , s.supply_breakdown_json
    
    -- Add BTC pricing for USD valuation
    , p.price as btc_price_usd
    , cast(coalesce(cast(s.total_btc_supply as double) * p.price, 0) as decimal(38,8)) as total_btc_usd_value
    
from {{ ref('sui_tvl_supply_silver') }} s

-- Join BTC pricing (using TBTC token, latest price per day to prevent duplication)
left join (
    select 
        blockchain
        , date(minute) as price_date
        , price
        , row_number() over (partition by blockchain, date(minute) order by minute desc) as rn
    from {{ source('prices','usd') }}
    where blockchain = 'sui'
    and contract_address = cast(
        regexp_replace(
            split_part(
                lower('0x77045f1b9f811a7a8fb9ebd085b5b0c55c5cb0d1520ff55f7037f89b5da9f5f1::TBTC::TBTC')
                --using TBTC token which has good pricing coverage instead of BTC
                , '::', 1
            )
            , '^0x0*([0-9a-f]+)$', '0x$1'
        ) as varbinary
    )
) p on p.blockchain = 'sui'
    and p.price_date = s.block_date
    and p.rn = 1

{% if is_incremental() %}
where {{ incremental_predicate('s.block_date') }}
{% endif %}

order by block_date desc 