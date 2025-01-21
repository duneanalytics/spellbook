{{ config(
    schema = 'avantis_base',
    alias = 'perpetual_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["base"]\',
                                spell_type = "project",
                                spell_name = "avantis",
                                contributors = \'["princi"]\') }}'
)
}}

-- {% set project_start_date = '2024-01-01' %}

WITH trading_events AS (
    -- OpenLimitPlaced events
    SELECT
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        evt_tx_from,
        evt_tx_to,
        evt_index,
        contract_address,
        trader,
        pairIndex,
        index as position_size,
        openPrice / 1e8 as price_usd,
        executionFee / 1e18 as fee_usd,
        isBuy,
        'limit_order' as event_type
    FROM {{ source('avantis_base', 'Trading_evt_OpenLimitPlaced') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL

    -- MarginUpdated events
    SELECT
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        evt_tx_from,
        evt_tx_to,
        evt_index,
        contract_address,
        trader,
        pairIndex,
        index as position_size,
        NULL as price_usd,  -- No direct price equivalent
        marginFees / 1e18 as fee_usd,
        CASE 
            WHEN newTrade = 'Long' THEN true
            WHEN newTrade = 'Short' THEN false
            ELSE NULL
        END as isBuy,
        'margin_update' as event_type
    FROM {{ source('avantis_base', 'Trading_evt_MarginUpdated') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

perps AS (
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        CASE pairIndex
            WHEN 0 THEN 'BTC-USD'
            WHEN 1 THEN 'ETH-USD'
            WHEN 2 THEN 'LINK-USD'
            WHEN 3 THEN 'DOGE-USD'
            WHEN 4 THEN 'MATIC-USD'
            WHEN 5 THEN 'ADA-USD'
            WHEN 6 THEN 'SUSHI-USD'
            WHEN 7 THEN 'AAVE-USD'
            WHEN 8 THEN 'ALGO-USD'
            WHEN 9 THEN 'BAT-USD'
            WHEN 10 THEN 'COMP-USD'
            WHEN 11 THEN 'DOT-USD'
            WHEN 12 THEN 'EOS-USD'
            WHEN 13 THEN 'LTC-USD'
            WHEN 14 THEN 'MANA-USD'
            WHEN 15 THEN 'OMG-USD'
            WHEN 16 THEN 'SNX-USD'
            WHEN 17 THEN 'UNI-USD'
            WHEN 18 THEN 'XLM-USD'
            WHEN 19 THEN 'XRP-USD'
            WHEN 20 THEN 'ZEC-USD'
            WHEN 31 THEN 'LUNA-USD'
            WHEN 32 THEN 'YFI-USD'
            WHEN 33 THEN 'SOL-USD'
            -- Add more trading pairs as needed
            ELSE CONCAT('pair_index_', CAST(pairIndex as VARCHAR))
        END AS virtual_asset,

        CASE pairIndex
           WHEN 0 THEN 'BTC-USD'
            WHEN 1 THEN 'ETH-USD'
            WHEN 2 THEN 'LINK-USD'
            WHEN 3 THEN 'DOGE-USD'
            WHEN 4 THEN 'MATIC-USD'
            WHEN 5 THEN 'ADA-USD'
            WHEN 6 THEN 'SUSHI-USD'
            WHEN 7 THEN 'AAVE-USD'
            WHEN 8 THEN 'ALGO-USD'
            WHEN 9 THEN 'BAT-USD'
            WHEN 10 THEN 'COMP-USD'
            WHEN 11 THEN 'DOT-USD'
            WHEN 12 THEN 'EOS-USD'
            WHEN 13 THEN 'LTC-USD'
            WHEN 14 THEN 'MANA-USD'
            WHEN 15 THEN 'OMG-USD'
            WHEN 16 THEN 'SNX-USD'
            WHEN 17 THEN 'UNI-USD'
            WHEN 18 THEN 'XLM-USD'
            WHEN 19 THEN 'XRP-USD'
            WHEN 20 THEN 'ZEC-USD'
            WHEN 31 THEN 'LUNA-USD'
            WHEN 32 THEN 'YFI-USD'
            WHEN 33 THEN 'SOL-USD'
            -- Add more trading pairs as needed
            ELSE CONCAT('pair_index_', CAST(pairIndex as VARCHAR))
        END AS underlying_asset,

        CASE pairIndex
            WHEN 0 THEN 'BTC-USD'
            WHEN 1 THEN 'ETH-USD'
            WHEN 2 THEN 'LINK-USD'
            WHEN 3 THEN 'DOGE-USD'
            WHEN 4 THEN 'MATIC-USD'
            WHEN 5 THEN 'ADA-USD'
            WHEN 6 THEN 'SUSHI-USD'
            WHEN 7 THEN 'AAVE-USD'
            WHEN 8 THEN 'ALGO-USD'
            WHEN 9 THEN 'BAT-USD'
            WHEN 10 THEN 'COMP-USD'
            WHEN 11 THEN 'DOT-USD'
            WHEN 12 THEN 'EOS-USD'
            WHEN 13 THEN 'LTC-USD'
            WHEN 14 THEN 'MANA-USD'
            WHEN 15 THEN 'OMG-USD'
            WHEN 16 THEN 'SNX-USD'
            WHEN 17 THEN 'UNI-USD'
            WHEN 18 THEN 'XLM-USD'
            WHEN 19 THEN 'XRP-USD'
            WHEN 20 THEN 'ZEC-USD'
            WHEN 31 THEN 'LUNA-USD'
            WHEN 32 THEN 'YFI-USD'
            WHEN 33 THEN 'SOL-USD'
            -- Add more trading pairs as needed
            ELSE CONCAT('pair_index_', CAST(pairIndex as VARCHAR))
        END AS market,

        contract_address AS market_address,
        COALESCE(price_usd * position_size, position_size) AS volume_usd,
        fee_usd,
        position_size AS margin_usd,

        CASE 
            WHEN event_type = 'limit_order' THEN
                CASE WHEN isBuy THEN 'long' ELSE 'short' END
            WHEN event_type = 'margin_update' THEN
                CASE WHEN isBuy THEN 'update_long' ELSE 'update_short' END
        END AS trade,

        'avantis' AS project,
        '1' AS version,
        'avantis' AS frontend,
        trader,
        COALESCE(price_usd * position_size, position_size) AS volume_raw,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM trading_events
)

SELECT
    'base' AS blockchain,
    CAST(date_trunc('DAY', perps.block_time) AS date) AS block_date,
    CAST(date_trunc('MONTH', perps.block_time) AS date) AS block_month,
    perps.block_time,
    perps.virtual_asset,
    perps.underlying_asset,
    perps.market,
    perps.market_address,
    perps.volume_usd,
    perps.fee_usd,
    perps.margin_usd,
    perps.trade,
    perps.project,
    perps.version,
    perps.frontend,
    perps.trader,
    CAST(perps.volume_raw as UINT256) as volume_raw,
    perps.tx_hash,
    tx."from" AS tx_from,
    tx."to" AS tx_to,
    perps.evt_index
FROM perps
INNER JOIN {{ source('base', 'transactions') }} AS tx
    ON perps.tx_hash = tx.hash
    AND perps.block_number = tx.block_number
    AND tx.block_time >= DATE '2024-01-01'
    