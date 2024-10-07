{{ config(
    alias='base_trades',
    schema='unidex_optimism',
    partition_by=['block_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(blockchains = \'["optimism"]\',
                                spell_type = "project",
                                spell_name = "unidex",
                                contributors = \'["ARDev097", "hosuke"]\') }}'
    )
}}

{% set project_start_date = '2023-03-08' %}
{% set unidex_optimism_evt_trade_tables = [
    source('unidex_optimism', 'metaaggregator_settlement_evt_Trade')
] %}

with dexs as (
    {% for evt_trade_table in unidex_optimism_evt_trade_tables %}
        SELECT
            evt_block_time          AS block_time,
            owner                   AS taker,
            CAST(NULL as VARBINARY) AS maker,
            buyAmount             AS token_bought_amount_raw,
            sellAmount            AS token_sold_amount_raw,
            CAST(NULL AS double)    AS amount_usd,
            buyToken                AS token_bought_address,
            sellToken                AS token_sold_address,
            contract_address        AS project_contract_address,
            evt_tx_hash             AS tx_hash,
            array[-1]               AS trace_address,
            evt_index
        FROM {{ evt_trade_table }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}

        {% if not loop.last %}
        UNION ALL
        {% endif %}

    {% endfor %}
)

SELECT
    'optimism'                                             AS blockchain,
    'unidex'                                     AS project,
    '1'                                                    AS version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) AS date)    AS block_date,
    TRY_CAST(date_trunc('MONTH', dexs.block_time) AS date)  AS block_month,
    dexs.block_time,
    dexs.token_bought_amount_raw                              AS token_bought_amount_raw,
    dexs.token_sold_amount_raw                               AS token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    COALESCE(dexs.taker, tx."from")                        AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx."from"                                              AS tx_from,
    tx.to                                                  AS tx_to,
    dexs.evt_index,
    dexs.trace_address
FROM dexs
INNER JOIN {{ source('optimism', 'transactions') }} tx
    ON dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND {{ incremental_predicate('tx.block_time') }}
    {% endif %}