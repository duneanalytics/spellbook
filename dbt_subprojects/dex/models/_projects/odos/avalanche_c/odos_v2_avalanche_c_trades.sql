{{
    config(
        schema = 'odos_avalanche_c',
        alias = 'trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
		post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "odos",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2023-12-19' %}
{% set blockchain = 'avalanche_c' %}
{% set native_token_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' %}

WITH dexs AS (
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        sender AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        amountOut AS token_bought_amount_raw,
        inputAmount AS token_sold_amount_raw,
        CASE
            WHEN outputToken = 0x0000000000000000000000000000000000000000 THEN {{native_token_address}}
            ELSE outputToken
        END AS token_bought_address,
        CASE
            WHEN inputToken = 0x0000000000000000000000000000000000000000 THEN {{native_token_address}}
            ELSE inputToken
        END AS token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM
        {{ source('odos_v2_avalanche_c', 'OdosRouterV2_evt_Swap') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% else %}
    WHERE
        evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)

SELECT
    '{{blockchain}}' AS blockchain,
    'odos' AS project,
    '2' AS version,
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
    dexs.evt_index,
    CAST(ARRAY[-1] AS ARRAY<bigint>) AS trace_address
FROM
    dexs
