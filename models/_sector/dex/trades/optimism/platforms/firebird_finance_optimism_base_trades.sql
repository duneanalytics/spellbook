{{ config(
    schema = 'firebird_finance_optimism',
    alias = 'base_trades',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set firebird_finance_optimism_evt_trade_tables = [
    source('firebird_finance_optimism', 'FireBirdRouter_evt_Swapped')
] %}

with dexs as (
    {% for evt_trade_table in firebird_finance_optimism_evt_trade_tables %}
        SELECT
            evt_block_time          AS block_time,
            sender                  AS taker,
            CAST(NULL as VARBINARY) AS maker,
            spentAmount             AS token_bought_amount_raw,
            returnAmount            AS token_sold_amount_raw,
            CAST(NULL AS double)    AS amount_usd,
            srcToken                AS token_bought_address,
            dstToken                AS token_sold_address,
            contract_address        AS project_contract_address,
            evt_tx_hash             AS tx_hash,
            evt_block_number        AS block_number,
            evt_index
        FROM {{ evt_trade_table }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('evt_block_time')}}
        {% endif %}

        {% if not loop.last %}
        UNION ALL
        {% endif %}

    {% endfor %}
)

SELECT
    'optimism'                                             AS blockchain,
    'firebird_finance'                                     AS project,
    '1'                                                    AS version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) AS date)    AS block_date,
    TRY_CAST(date_trunc('MONTH', dexs.block_time) AS date)  AS block_month,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs
