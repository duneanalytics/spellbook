{{ config(
    alias = 'lemma_trades',
    schema = 'lemma_finance_optimism',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
) }}

{% set project_start_date = '2023-10-17' %}

WITH all_events AS (
    SELECT
        evt_block_time,
        evt_block_number,
        evt_block_date,
        evt_index,
        contract_address,
        evt_tx_hash,
        sender,
        receiver,
        assets,
        shares,
        'Open Long' AS trade_type,
        'Open Long' AS trade
    FROM {{ source('lemma_finance_optimism', 'xlemmasynth_evt_deposit') }}
    {% if not is_incremental() %}
        WHERE evt_block_time >= DATE '{{ project_start_date }}'
    {% else %}
        WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL

    SELECT
        evt_block_time,
        evt_block_number,
        evt_block_date,
        evt_index,
        contract_address,
        evt_tx_hash,
        sender,
        receiver,
        assets,
        shares,
        'Close Long' AS trade_type,
        'Close Long' AS trade
    FROM {{ source('lemma_finance_optimism', 'xlemmasynth_evt_withdraw') }}
    {% if not is_incremental() %}
        WHERE evt_block_time >= DATE '{{ project_start_date }}'
    {% else %}
        WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

with_token_info AS (
    SELECT
        e.*,
        t.symbol AS asset_symbol,
        t.decimals
    FROM all_events e
    LEFT JOIN {{ source('tokens', 'erc20') }} t
        ON e.contract_address = t.contract_address
        AND t.blockchain = 'optimism'
),

final AS (
    SELECT
        'optimism' AS blockchain,
        CAST(date_trunc('day', evt_block_time) AS date) AS block_date,
        CAST(date_trunc('month', evt_block_time) AS date) AS block_month,
        evt_block_time AS block_time,
        contract_address AS market_address,
        CAST(NULL AS VARCHAR) AS market,
        asset_symbol AS virtual_asset,
        asset_symbol AS underlying_asset,
        CAST(assets / pow(10, COALESCE(decimals, 18)) AS DOUBLE) AS volume_usd,
        CAST(NULL AS DOUBLE) AS fee_usd,
        CAST(NULL AS DOUBLE) AS margin_usd,
        trade_type,
        trade,
        'lemma_finance' AS project,
        'v1' AS version,
        'lemma_finance' AS frontend,
        sender AS trader,
        assets AS volume_raw,
        evt_tx_hash AS tx_hash,
        sender AS tx_from,
        receiver AS tx_to,
        evt_index
    FROM with_token_info
)

SELECT * FROM final