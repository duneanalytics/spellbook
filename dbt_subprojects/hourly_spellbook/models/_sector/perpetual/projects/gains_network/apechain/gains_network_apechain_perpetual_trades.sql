{{ config(
    schema = 'gains_network_apechain',
    alias = 'perpetual_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(blockchains = \'["apechain"]\',
                                spell_type = "project",
                                spell_name = "gains_network",
                                contributors = \'["principatel"]\') }}'
)
}}

-- {% set project_start_date = '2024-01-01' %}

WITH position_changes AS (
    -- Position Size Decreases
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
        long,
        collateralDelta,
        collateralPriceUsd,
        oraclePrice,
        leverageDelta,
        "values" as value_data,
        'decrease' as action
    FROM {{source('gains_network_apechain','gnsmulticollatdiamond_evt_positionsizedecreaseexecuted')}}
        {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
        {% else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
    UNION ALL

    -- Position Size Increases
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
        long,
        collateralDelta,
        collateralPriceUsd,
        oraclePrice,
        leverageDelta,
        "values" as value_data,
        'increase' as action
    FROM {{ source('gains_network_apechain','gnsmulticollatdiamond_evt_positionsizeincreaseexecuted')}}
        {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
        {% else %}
    WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
),

transactions_filtered AS (
    SELECT
        hash,
        block_number,
        "from",
        "to",
        block_time
    FROM {{ source('apechain', 'transactions') }}
    WHERE {% if is_incremental() %}
        {{ incremental_predicate('block_time') }}
    {% else %}
        block_time >= TIMESTAMP '{{project_start_date}}'
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
            ELSE 'pair_index_' || CAST(pairIndex AS VARCHAR)
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
            ELSE 'pair_index_' || CAST(pairIndex AS VARCHAR)
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
            ELSE 'pair_index_' || CAST(pairIndex AS VARCHAR)
        END AS market,

        contract_address AS market_address,
        (collateralDelta * collateralPriceUsd * leverageDelta) / 1e36 AS volume_usd,
        CAST(JSON_EXTRACT(value_data, '$.vaultFeeCollateral') AS double) / 1e18 AS fee_usd, 
        collateralDelta / 1e18 AS margin_usd,

        CASE 
            WHEN action = 'increase' AND long = true THEN 'long'
            WHEN action = 'increase' AND long = false THEN 'short'
            WHEN action = 'decrease' AND long = true THEN 'close_long'
            WHEN action = 'decrease' AND long = false THEN 'close_short'
        END AS trade,

        'gains_network' AS project,
        '1' AS version,
        'gains_network' AS frontend,
        trader,
        collateralDelta * leverageDelta AS volume_raw,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM position_changes
)

SELECT
    'apechain' AS blockchain,
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
INNER JOIN transactions_filtered tx
    ON perps.tx_hash = tx.hash
    AND perps.block_number = tx.block_number