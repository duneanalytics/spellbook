{{ config(
    
    alias = 'perpetual_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "gmx",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2021-12-22' %}

WITH 

perp_events as (
    -- decrease position
    SELECT
        evt_block_time as block_time, 
        evt_block_number as block_number, 
        'decrease_position' as trade_data, 
        indexToken as virtual_asset,
        collateralToken as underlying_asset,
        sizeDelta/1E30 as volume_usd, 
        fee/1E30 as fee_usd, 
        collateralDelta/1E30 as margin_usd,
        CAST(sizeDelta as UINT256) as volume_raw, 
        CASE WHEN isLong = false THEN 'short' ELSE 'long' END as trade_type, 
        account as trader, 
        contract_address as market_address, 
        evt_index,
        evt_tx_hash as tx_hash 
    FROM 
    {{ source('gmx_avalanche_c', 'Vault_evt_DecreasePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}

    UNION ALL 

    -- increase position  
    SELECT
        evt_block_time as block_time, 
        evt_block_number as block_number, 
        'increase_position' as trade_data, 
        indexToken as virtual_asset,
        collateralToken as underlying_asset,
        sizeDelta/1E30 as volume_usd, 
        fee/1E30 as fee_usd, 
        collateralDelta/1E30 as margin_usd,
        CAST(sizeDelta as UINT256) as volume_raw, 
        CASE WHEN isLong = false THEN 'short' ELSE 'long' END as trade_type, 
        account as trader, 
        contract_address as market_address, 
        evt_index,
        evt_tx_hash as tx_hash 
    FROM 
    {{ source('gmx_avalanche_c', 'Vault_evt_IncreasePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}

    UNION ALL 

    -- liquidate position 
    SELECT
        evt_block_time as block_time, 
        evt_block_number as block_number, 
        'liquidate_position' as trade_data, 
        indexToken as virtual_asset,
        collateralToken as underlying_asset,
        size/1E30 as volume_usd, 
        0 as fee_usd, 
        collateral/1E30 as margin_usd,
        CAST(size as UINT256) as volume_raw, 
        CASE WHEN isLong = false THEN 'short' ELSE 'long' END as trade_type, 
        account as trader, 
        contract_address as market_address, 
        evt_index,
        evt_tx_hash as tx_hash 
    FROM 
    {{ source('gmx_avalanche_c', 'Vault_evt_LiquidatePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}
)

SELECT 
    'avalanche_c' as blockchain, 
    'gmx' as project, 
    '1' as version, 
    'gmx' as frontend,
    CAST(date_trunc('day', pe.block_time) as date) as block_date, 
    CAST(date_trunc('month', pe.block_time) as date) as block_month,
    pe.block_time, 
    COALESCE(erc20a.symbol, CAST(pe.virtual_asset as VARCHAR)) as virtual_asset, 
    COALESCE(erc20b.symbol, CAST(pe.underlying_asset as VARCHAR)) as underlying_asset, 
    CASE 
        WHEN pe.virtual_asset = pe.underlying_asset THEN COALESCE(erc20a.symbol, CAST(pe.virtual_asset as VARCHAR))
        ELSE COALESCE(erc20a.symbol, CAST(pe.virtual_asset as VARCHAR)) || '-' || COALESCE(erc20b.symbol, CAST(pe.underlying_asset as VARCHAR))
    END as market, 
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    pe.margin_usd,
    CASE 
        WHEN pe.trade_data = 'increase_position' THEN 'open' || '-' || pe.trade_type
        WHEN pe.trade_data = 'decrease_position' THEN 'close' || '-' || pe.trade_type
        WHEN pe.trade_data = 'liquidate_position' THEN 'liquidate' || '-' || pe.trade_type
    END as trade, 
    pe.trader, 
    pe.volume_raw,
    pe.tx_hash,
    txns."to" as tx_to,
    txns."from" as tx_from,
    pe.evt_index
FROM 
perp_events pe 
INNER JOIN {{ source('avalanche_c', 'transactions') }} txns 
    ON pe.tx_hash = txns.hash
    AND pe.block_number = txns.block_number
    {% if not is_incremental() %}
    AND txns.block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND txns.block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = pe.virtual_asset
    AND erc20a.blockchain = 'avalanche_c'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = pe.underlying_asset
    AND erc20b.blockchain = 'avalanche_c'
