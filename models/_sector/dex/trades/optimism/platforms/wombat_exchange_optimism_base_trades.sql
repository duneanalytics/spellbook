{{ config(
    
    schema = 'wombat_exchange_optimism',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}
  


{% set project_start_date = '2023-09-22' %}
{% set wombat_exchange_optimism_evt_trade_tables = [
    source('wombat_exchange_optimism', 'CrossChainPool_evt_SwapV2')
] %}

WITH dexs AS
(
    {% for evt_trade_table in wombat_exchange_optimism_evt_trade_tables %}

    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.evt_tx_to AS taker
        , t.evt_tx_from AS maker
        , t.toAmount AS token_sold_amount_raw
        , t.fromAmount AS token_bought_amount_raw
        , t.toToken AS token_sold_address
        , t.fromToken AS token_bought_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
    FROM
        {{ evt_trade_table }} t 
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
     {% if not loop.last %}
      UNION ALL
        {% endif %}

    {% endfor %}
)

SELECT
    'optimism' AS blockchain
    , 'wombat_exchange' AS project
    , '2' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , dexs.token_bought_amount_raw
    , dexs.token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM
    dexs