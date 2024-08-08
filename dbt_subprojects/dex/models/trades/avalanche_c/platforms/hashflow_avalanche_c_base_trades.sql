{{
    config(
        schema = 'hashflow_avalanche_c',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2022-04-11' %}
{% set hashflow_bnb_evt_trade_tables = [
    source('hashflow_avalanche_c', 'Pool_evt_Trade')
    , source('hashflow_avalanche_c', 'Pool_evt_LzTrade')
    , source('hashflow_avalanche_c', 'Pool_evt_XChainTrade')
] %}

with dexs AS (
    {% for evt_trade_table in hashflow_bnb_evt_trade_tables %}
        SELECT
            evt_block_time          AS block_time,
            trader                  AS taker,
            CAST(NULL as VARBINARY) AS maker,
            quoteTokenAmount        AS token_bought_amount_raw,
            baseTokenAmount         AS token_sold_amount_raw,
            quoteToken              AS token_bought_address,
            baseToken               AS token_sold_address,
            contract_address        AS project_contract_address,
            evt_tx_hash             AS tx_hash,
            evt_index,
            evt_block_number        AS block_number
        FROM {{ evt_trade_table }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('evt_block_time')}}
        {% else %}
        WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}

        {% if not loop.last %}
        UNION ALL
        {% endif %}

    {% endfor %}
)

select
    'avalanche_c' as blockchain,
    'hashflow' as project,
    '1' as version,
    CAST(date_trunc('month', dexs.block_time) as date) as block_month,
    CAST(date_trunc('day', dexs.block_time) as date) as block_date,
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
from dexs
