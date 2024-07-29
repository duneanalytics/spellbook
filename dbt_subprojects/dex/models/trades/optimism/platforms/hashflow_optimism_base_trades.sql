{{ config(
    alias = 'base_trades'
    ,schema = 'hashflow_optimism'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['tx_hash', 'evt_index']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set hashflow_optimism_evt_trade_tables = [
    source('hashflow_optimism', 'Pool_evt_Trade')
    , source('hashflow_optimism', 'Pool_evt_LzTrade')
    , source('hashflow_optimism', 'Pool_evt_XChainTrade')
] %}


with dexs AS (
    {% for evt_trade_table in hashflow_optimism_evt_trade_tables %}
        SELECT
            evt_block_time          AS block_time,
            trader                  AS taker,
            CAST(NULL as VARBINARY) AS maker,
            quoteTokenAmount        AS token_bought_amount_raw,
            baseTokenAmount         AS token_sold_amount_raw,
            CAST(NULL AS double)    AS amount_usd,
            quoteToken              AS token_bought_address,
            baseToken               AS token_sold_address,
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
    'hashflow'                                                AS project,
    '1'                                                       AS version,
    CAST(date_trunc('DAY', dexs.block_time) AS date)          AS block_date,
    CAST(date_trunc('MONTH', dexs.block_time) AS date)        AS block_month,
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

