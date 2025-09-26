{{ config(
    schema='sui_tvl',
    alias='btc_ecosystem',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['protocol', 'market_id', 'coin_type', 'block_date'],
    partition_by=['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags=['sui','tvl','btc','ecosystem']
) }}

-- BTC Ecosystem: Granular token-level TVL data across Supply, Lending, and DEX
-- Each row represents one token in one object (pool/market)

with dex_pools_unpivoted as (
    -- DEX pools unpivoted: one row per token per pool
    select 
        d.protocol,
        d.pool_id as market_id,
        d.coin_type_a as coin_type,
        d.coin_a_symbol as token_symbol,
        d.pool_name as token_name,
        'Liquidity Pool' as object_type,
        d.avg_coin_a_amount as tvl_native_amount,
        -- Calculate individual token TVL (half of pool TVL since it's token A)
        cast(coalesce(cast(d.avg_coin_a_amount as double) * d.coin_a_price_usd, 0) as decimal(38,8)) as tvl_usd,
        d.fee_rate_percent as protocol_fee_rate,
        d.metric_date as block_date
    from {{ ref('sui_tvl_dex_pools_gold') }} d
    {% if is_incremental() %}
    where {{ incremental_predicate('d.metric_date') }}
    {% endif %}
    
    union all
    
    select 
        d.protocol,
        d.pool_id as market_id,
        d.coin_type_b as coin_type,
        d.coin_b_symbol as token_symbol,
        d.pool_name as token_name,
        'Liquidity Pool' as object_type,
        d.avg_coin_b_amount as tvl_native_amount,
        -- Calculate individual token TVL (half of pool TVL since it's token B)
        cast(coalesce(cast(d.avg_coin_b_amount as double) * d.coin_b_price_usd, 0) as decimal(38,8)) as tvl_usd,
        d.fee_rate_percent as protocol_fee_rate,
        d.metric_date as block_date
    from {{ ref('sui_tvl_dex_pools_gold') }} d
    {% if is_incremental() %}
    where {{ incremental_predicate('d.metric_date') }}
    {% endif %}
),

lending_markets_unpivoted as (
    -- Lending markets: one row per token type (collateral) per market
    select 
        l.protocol,
        concat(l.protocol, '_', l.collateral_coin_symbol) as market_id, -- Create unique market identifier
        l.collateral_coin_type as coin_type,
        l.collateral_coin_symbol as token_symbol,
        concat(l.protocol, ' - ', l.collateral_coin_symbol, ' Market') as token_name,
        case 
            when l.protocol = 'bucket' then 'Collateral Vault'
            else 'Lending Market'
        end as object_type,
        l.btc_collateral as tvl_native_amount,
        -- Lending TVL is the collateral amount (what's locked)
        cast(coalesce(cast(l.btc_collateral as double) * btc_price.price, 0) as decimal(38,8)) as tvl_usd,
        cast(null as decimal) as protocol_fee_rate, -- Lending doesn't have explicit fee rates
        l.date as block_date
    from {{ ref('sui_tvl_lending_pools_gold') }} l
    
    -- Get BTC price for lending TVL calculation
    left join {{ source('prices','usd') }} btc_price
        on btc_price.blockchain = 'sui'
        and date(btc_price.minute) = l.date
        and btc_price.contract_address = cast(
            regexp_replace(
                split_part(
                    lower('0x027792d9fed7f9844eb4839566001bb6f6cb4804f66aa2da6fe1ee242d896881::coin::COIN'), 
                    '::', 1
                ),
                '^0x0*([0-9a-f]+)$', '0x$1'
            ) as varbinary
        )
    
    {% if is_incremental() %}
    where {{ incremental_predicate('l.date') }}
    {% endif %}
)

select
    protocol,
    object_type,
    market_id,
    coin_type,
    token_symbol,
    token_name,
    tvl_native_amount,
    tvl_usd,
    protocol_fee_rate,
    block_date

from dex_pools_unpivoted

union all

select
    protocol,
    object_type,
    market_id,
    coin_type,
    token_symbol,
    token_name,
    tvl_native_amount,
    tvl_usd,
    protocol_fee_rate,
    block_date

from lending_markets_unpivoted

where tvl_native_amount > 0 -- Only include objects with actual TVL
  and tvl_usd > 1000 -- Business filter for meaningful TVL

order by 
    block_date desc,
    tvl_usd desc