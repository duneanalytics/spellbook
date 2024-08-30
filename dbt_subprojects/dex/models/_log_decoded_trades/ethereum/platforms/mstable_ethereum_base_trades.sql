{{
    config(
        schema = 'mstable_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}

{%
    set sources = [
        {'version': 'masset', 'source': 'Masset_evt_Swapped'},
        {'version': 'feederpool', 'source': 'FeederPool_evt_Swapped'},
    ]
%}

WITH dexs AS (
    {% for src in sources %}
        SELECT
            '{{ src["version"] }}' as version,
            t.evt_block_number AS block_number,
            t.evt_block_time AS block_time,
            t.swapper AS taker,
            CAST(NULL as VARBINARY) as maker,
            t.outputAmount AS token_bought_amount_raw,
            CAST(NULL AS UINT256) AS token_sold_amount_raw,
            CASE
                WHEN t.output = 0x0000000000000000000000000000000000000000 THEN {{ weth_address }}
                ELSE t.output
            END AS token_bought_address,
            CASE
                WHEN t.input = 0x0000000000000000000000000000000000000000 THEN {{ weth_address }}
                ELSE t.input
            END AS token_sold_address,
            t.contract_address AS project_contract_address,
            t.evt_tx_hash AS tx_hash,
            t.evt_index
        FROM {{ source('mstable_ethereum', src["source"] )}} t
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT
    'ethereum' AS blockchain,
    'mstable' AS project,
    dexs.version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
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
