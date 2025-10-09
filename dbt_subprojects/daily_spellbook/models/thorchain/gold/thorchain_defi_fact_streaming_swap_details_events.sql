{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_streaming_swap_details_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'streaming_swaps', 'fact']
) }}

-- DeFi fact table for Thorchain streaming swap details events
-- Provides detailed streaming swap execution data
SELECT
    ssd.tx_id as tx_hash,
    ssd.block_time,
    b.block_date,
    b.block_month,
    b.block_hour,
    b.height as block_height,
    
    -- Streaming swap details
    ssd.streaming_interval,
    ssd.quantity as quantity_per_stream,
    ssd.stream_count,
    ssd.last_height,
    
    -- Deposit details
    ssd.deposit_asset as deposit_token_symbol,
    ssd.deposit_amount as deposit_token_amount,
    ssd.deposit_e8 as deposit_token_amount_raw,
    ssd.deposit_contract_address as deposit_token_address,
    
    -- Input details (actual trade input)
    ssd.in_asset as token_sold_symbol,
    ssd.in_amount as token_sold_amount,
    ssd.in_e8 as token_sold_amount_raw,
    ssd.in_contract_address as token_sold_address,
    
    -- Output details (actual trade output)
    ssd.out_asset as token_bought_symbol,
    ssd.out_amount as token_bought_amount,
    ssd.out_e8 as token_bought_amount_raw,
    ssd.out_contract_address as token_bought_address,
    
    -- Failed swap information
    ssd.failed_swaps,
    ssd.failed_swap_reasons,
    
    -- USD values
    COALESCE(dp.price * ssd.deposit_amount, 0) as deposit_amount_usd,
    COALESCE(ip.price * ssd.in_amount, 0) as amount_usd_sold,
    COALESCE(op.price * ssd.out_amount, 0) as amount_usd_bought,
    
    -- DEX aggregator fields for compatibility
    'thorchain' as project,
    '1' as version,
    'streaming_swap' as category,
    ssd.event_id,
    
    -- Streaming swap metrics
    CASE 
        WHEN ssd.stream_count > 0 THEN ssd.deposit_amount / ssd.stream_count
        ELSE ssd.deposit_amount
    END as average_amount_per_stream,
    
    CASE 
        WHEN COALESCE(ip.price * ssd.in_amount, 0) > 0 
        THEN COALESCE(op.price * ssd.out_amount, 0) / COALESCE(ip.price * ssd.in_amount, 1)
        ELSE null 
    END as price_impact,
    
    -- Volume and value metrics  
    GREATEST(COALESCE(ip.price * ssd.in_amount, 0), COALESCE(op.price * ssd.out_amount, 0)) as volume_usd,
    
    -- Success rate metrics
    CASE 
        WHEN ssd.failed_swaps IS NOT NULL AND cardinality(ssd.failed_swaps) > 0 THEN false
        ELSE true
    END as is_successful,
    
    COALESCE(cardinality(ssd.failed_swaps), 0) as failed_swap_count,
    
    -- Trade direction relative to RUNE
    CASE
        WHEN ssd.in_asset LIKE 'THOR.RUNE%' THEN 'sell_rune'
        WHEN ssd.out_asset LIKE 'THOR.RUNE%' THEN 'buy_rune'
        ELSE 'asset_to_asset'
    END as trade_direction

FROM {{ ref('thorchain_silver_streaming_swap_details_events') }} ssd
LEFT JOIN {{ ref('thorchain_core_dim_block') }} b
    ON ssd.block_time >= b.block_time
    AND ssd.block_time < b.block_time + interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_prices') }} dp
    ON dp.contract_address = ssd.deposit_contract_address
    AND dp.block_time <= ssd.block_time
    AND dp.block_time >= ssd.block_time - interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_prices') }} ip
    ON ip.contract_address = ssd.in_contract_address
    AND ip.block_time <= ssd.block_time
    AND ip.block_time >= ssd.block_time - interval '1' hour
LEFT JOIN {{ ref('thorchain_silver_prices') }} op
    ON op.contract_address = ssd.out_contract_address
    AND op.block_time <= ssd.block_time
    AND op.block_time >= ssd.block_time - interval '1' hour

{% if is_incremental() %}
WHERE {{ incremental_predicate('ssd.block_time') }}
  AND ssd.block_time >= current_date - interval '7' day
{% endif %}
