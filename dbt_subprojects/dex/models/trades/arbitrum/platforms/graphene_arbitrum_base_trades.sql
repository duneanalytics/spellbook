{{ 
    config(
        schema = 'graphene_multichain',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    ) 
}}

WITH dexs AS (
    SELECT
        'graphene' AS blockchain,
        'carbon' AS project,
        '1' AS version,
        CAST(DATE_TRUNC('month', evt_block_time) AS DATE) AS block_month,
        CAST(DATE_TRUNC('day', evt_block_time) AS DATE) AS block_date,
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        targetAmount AS token_bought_amount_raw,
        sourceAmount AS token_sold_amount_raw,
        targetToken AS token_bought_address,
        sourceToken AS token_sold_address,
        trader AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('graphene_multichain', 'carboncontroller_evt_tokenstraded') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT * FROM dexs
