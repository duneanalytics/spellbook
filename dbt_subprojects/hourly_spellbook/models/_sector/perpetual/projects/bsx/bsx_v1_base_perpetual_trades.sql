{{ config(
    alias = 'perpetual_trades',
    schema = 'bsx_v1_base',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    )
}}

{% set project_start_date = '2023-01-01' %} 

WITH 

perp_events as (
    -- open position events
    SELECT
        evt_block_time as block_time, 
        evt_block_number as block_number, 
        'open_position' as trade_data, 
        productId as product_id,
        account as trader, 
        contract_address as market_address, 
        evt_index,
        evt_tx_hash as tx_hash,
        CAST(fee AS DOUBLE)/1E18 as fee_usd, 
        CAST(NULL AS DOUBLE) as volume_usd,
        CAST(NULL AS DOUBLE) as pnl
    FROM 
    {{ source('bsx_base', 'BSX1000x_evt_OpenPosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    -- close position events
    SELECT
        evt_block_time as block_time, 
        evt_block_number as block_number, 
        'close_position' as trade_data, 
        productId as product_id,
        account as trader, 
        contract_address as market_address, 
        evt_index,
        evt_tx_hash as tx_hash,
        CAST(fee AS DOUBLE)/1E18 as fee_usd, 
        CAST(NULL AS DOUBLE) as volume_usd,
        CAST(pnl AS DOUBLE)/1E18 as pnl
    FROM 
    {{ source('bsx_base', 'BSX1000x_evt_ClosePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT 
    'base' as blockchain, 
    'bsx' as project, 
    '1' as version, 
    'bsx' as frontend,
    CAST(date_trunc('day', pe.block_time) as date) as block_date, 
    CAST(date_trunc('month', pe.block_time) as date) as block_month,
    pe.block_time, 
    CAST(NULL AS VARCHAR) as virtual_asset, 
    CAST(NULL AS VARCHAR) as underlying_asset, 
    pe.product_id as market,
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    CAST(NULL AS DOUBLE) as margin_usd,
    CASE 
        WHEN pe.trade_data = 'open_position' THEN 'open'
        WHEN pe.trade_data = 'close_position' THEN 'close'
    END as trade, 
    pe.trader, 
    CAST(NULL AS UINT256) as volume_raw,
    pe.tx_hash,
    txns."to" as tx_to,
    txns."from" as tx_from,
    pe.evt_index,
    pe.pnl
FROM 
perp_events pe 
INNER JOIN {{ source('base', 'transactions') }} txns 
    ON pe.tx_hash = txns.hash
    AND pe.block_number = txns.block_number
    {% if not is_incremental() %}
    AND txns.block_time >= DATE '{{project_start_date}}'
    {% else %}
    AND {{ incremental_predicate('txns.block_time') }}
    {% endif %}