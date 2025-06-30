{{ config(
    alias = 'perpetual_trades',
    schema = 'lyra_v1_optimism',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
)}}

{% set project_start_date = '2023-01-01' %}

WITH perp_events as (
    -- Open Position events
    SELECT 
        evt_block_time as block_time,
        evt_block_number as block_number,
        'open_position' as trade_data,
        listingId as product_id,
        trader,
        contract_address as market_address,
        evt_index,
        evt_tx_hash as tx_hash,
        CAST(totalCost AS DOUBLE) as fee_usd,
        CAST(amount AS DOUBLE) as volume_usd,
        tradeType
    FROM {{ source('lyra_v1_optimism', 'OptionMarket_evt_PositionOpened') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% else %}
    WHERE evt_block_time >= DATE '{{ project_start_date }}'
    {% endif %}

    UNION ALL

    -- Close Position events
    SELECT 
        evt_block_time as block_time,
        evt_block_number as block_number,
        'close_position' as trade_data,
        listingId as product_id,
        trader,
        contract_address as market_address,
        evt_index,
        evt_tx_hash as tx_hash,
        CAST(totalCost AS DOUBLE) as fee_usd,
        CAST(amount AS DOUBLE) as volume_usd,
        tradeType
    FROM {{ source('lyra_v1_optimism', 'OptionMarket_evt_PositionClosed') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% else %}
    WHERE evt_block_time >= DATE '{{ project_start_date }}'
    {% endif %}
)

SELECT 
    'optimism' as blockchain,
    'lyra' as project,
    '1' as version,
    'lyra' as frontend,
    CAST(date_trunc('day', pe.block_time) as date) as block_date,
    CAST(date_trunc('month', pe.block_time) as date) as block_month,
    pe.block_time,
    CAST(NULL AS VARCHAR) as virtual_asset,
    CAST(NULL AS VARCHAR) as underlying_asset,
    CAST(NULL AS VARCHAR) as market,
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    CAST(NULL AS DOUBLE) as margin_usd,
    CASE 
        WHEN pe.trade_data = 'open_position' THEN 'open'
        WHEN pe.trade_data = 'close_position' THEN 'close'
    END as trade,
    pe.trader,
    pe.tradeType,
    CAST(NULL AS UINT256) as volume_raw,
    pe.tx_hash,
    txns."to" as tx_to,
    txns."from" as tx_from,
    pe.evt_index,
    CAST(NULL AS DOUBLE) as pnl
FROM perp_events pe
INNER JOIN {{ source('optimism', 'transactions') }} txns 
    ON pe.tx_hash = txns.hash 
    AND pe.block_number = txns.block_number
{% if is_incremental() %}
    AND {{ incremental_predicate('txns.block_time') }}
{% else %}
    AND txns.block_time >= DATE '{{ project_start_date }}'
{% endif %}