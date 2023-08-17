{{ config(
    tags=['dunesql'],
    alias = alias('perpetual_trades'),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "hubble_exchange",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2022-08-09' %}

WITH 

perp_events as (
    SELECT evt_block_time                                              as block_time,
           evt_block_number                                            as block_number,
           CASE WHEN (CAST(baseAsset as double) * 1) >= 0 THEN 'long' ELSE 'short' END as trade_type,       -- negative baseAsset is for short and positive is for long
           'AVAX'                                                      as virtual_asset,    -- only AVAX can currently be traded on hubble exchange
           CAST(NULL as VARCHAR)                                                        as underlying_asset, -- there's no way to track the underlying asset as traders need to deposit into their margin account before they're able to trade which is tracked in a seperate event not tied to the margin positions opened.
           quoteAsset / 1E6                                            as volume_usd,
           CAST(NULL as double)                                        as fee_usd,          -- no event to track fees
           CAST(NULL as double)                                        as margin_usd,       -- no event to track margin
           CAST(quoteAsset as double)                                  as volume_raw,
           trader,
           contract_address                                            as market_address,
           evt_index,
           evt_tx_hash                                                 as tx_hash
    FROM 
    {{ source('hubble_exchange_avalanche_c', 'ClearingHouse_evt_PositionModified') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
), 

trade_data as (
    -- close position calls 
    SELECT call_block_number as block_number,
           call_tx_hash      as tx_hash,
           'close'           as trade_data
    FROM 
    {{ source('hubble_exchange_avalanche_c', 'ClearingHouse_call_closePosition') }}
    WHERE call_success = true 
    {% if is_incremental() %}
    AND call_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}

    UNION

    -- open position calls 
    SELECT
        call_block_number as block_number,
        call_tx_hash as tx_hash,
        'open' as trade_data
    FROM 
    {{ source('hubble_exchange_avalanche_c', 'ClearingHouse_call_openPosition') }}
    WHERE call_success = true 
    {% if is_incremental() %}
    AND call_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}

    UNION

    -- liquidate position events
    SELECT
        evt_block_number as block_number,
        evt_tx_hash as tx_hash,
        'liquidate' as trade_data
    FROM 
    {{ source('hubble_exchange_avalanche_c', 'ClearingHouse_evt_PositionLiquidated') }}
    WHERE 1 = 1
    {% if is_incremental() %}
    AND evt_block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
)

SELECT 'avalanche_c'                    as blockchain,
       'hubble_exchange'                as project,
       '1'                              as version,
       'hubble_exchange'                as frontend,
       CAST(date_trunc('day', pe.block_time) as date) as block_date,
       CAST(date_trunc('month', pe.block_time) as date) as block_month,
       pe.block_time,
       pe.virtual_asset,
       pe.underlying_asset,
       'AVAX'                           as market,
       pe.market_address,
       pe.volume_usd,
       pe.fee_usd,
       pe.margin_usd,
       COALESCE(
                   td.trade_data || '-' || pe.trade_type, -- using the call/open functions to classify trades
                   'adjust' || '-' || pe.trade_type
           )                            as trade,
       pe.trader,
       pe.volume_raw,
       pe.tx_hash,
       txns."to"                          as tx_to,
       txns."from"                        as tx_from,
       pe.evt_index
FROM 
perp_events pe 
INNER JOIN 
{{ source('avalanche_c', 'transactions') }} txns 
    ON pe.tx_hash = txns.hash
    AND pe.block_number = txns.block_number
    {% if not is_incremental() %}
    AND txns.block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND txns.block_time >= date_trunc('day', now() - interval '7' Day)
    {% endif %}
LEFT JOIN 
trade_data td 
    ON pe.block_number = td.block_number
    AND pe.tx_hash = td.tx_hash
