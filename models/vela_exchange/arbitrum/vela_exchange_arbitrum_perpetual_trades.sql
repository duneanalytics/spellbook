{{ config(
    alias = 'perpetual_trades',
    schema = 'vela_exchange_arbitrum',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    )
}}

{% set project_start_date = '2023-01-29' %}

WITH 

perp_events as (
    -- decrease position
    SELECT
        evt_block_time as block_time, 
        evt_block_number as block_number, 
        'decrease_position' as trade_data, 
        CAST(NULL AS VARCHAR) as virtual_asset,
        CAST(NULL AS VARCHAR) as underlying_asset,
        posData[2]/1E30 as volume_usd, 
        posData[5]/1E30 as fee_usd, 
        posData[1]/1E30 as margin_usd,
        CAST(posData[2] as UINT256) as volume_raw, 
        CASE WHEN isLong = false THEN 'short' ELSE 'long' END as trade_type, 
        account as trader, 
        contract_address as market_address, 
        evt_index,
        evt_tx_hash as tx_hash 
    FROM 
    {{ source('vela_arbitrum', 'VaultUtils_evt_DecreasePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- increase position  
    SELECT
        evt_block_time as block_time, 
        evt_block_number as block_number, 
        'increase_position' as trade_data, 
        CAST(NULL AS VARCHAR) as virtual_asset,
        CAST(NULL AS VARCHAR) as underlying_asset,
        posData[2]/1E30 as volume_usd, 
        posData[5]/1E30 as fee_usd, 
        posData[1]/1E30 as margin_usd,
        CAST(posData[2] as UINT256) as volume_raw, 
        CASE WHEN isLong = false THEN 'short' ELSE 'long' END as trade_type, 
        account as trader, 
        contract_address as market_address, 
        evt_index,
        evt_tx_hash as tx_hash 
    FROM 
    {{ source('vela_arbitrum', 'VaultUtils_evt_IncreasePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    
)





SELECT 
    'arbitrum' as blockchain, 
    'vela_exchange' as project, 
    '1' as version, 
    'vela_exchange' as frontend,
    CAST(date_trunc('day', pe.block_time) as date) as block_date, 
    CAST(date_trunc('month', pe.block_time) as date) as block_month,
    pe.block_time, 
    pe.virtual_asset as virtual_asset, 
    pe.underlying_asset as underlying_asset, 
    CAST(NULL AS VARCHAR) as market, 
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    pe.margin_usd,
    CASE 
        WHEN pe.trade_data = 'increase_position' THEN 'open' || '-' || pe.trade_type
        WHEN pe.trade_data = 'decrease_position' THEN 'close' || '-' || pe.trade_type
        -- WHEN pe.trade_data = 'liquidate_position' THEN 'liquidate' || '-' || pe.trade_type
    END as trade, 
    pe.trader, 
    pe.volume_raw,
    pe.tx_hash,
    txns."to" as tx_to,
    txns."from" as tx_from,
    pe.evt_index
FROM 
perp_events pe 
INNER JOIN {{ source('arbitrum', 'transactions') }} txns 
    ON pe.tx_hash = txns.hash
    AND pe.block_number = txns.block_number
    {% if not is_incremental() %}
    AND txns.block_time >= DATE '{{project_start_date}}'
    {% else %}
    AND {{ incremental_predicate('txns.block_time') }}
    {% endif %}