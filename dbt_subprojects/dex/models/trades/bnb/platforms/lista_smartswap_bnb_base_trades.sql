{{
    config(
        schema = 'lista_smartswap_bnb',
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
        e.evt_block_number AS block_number,
        e.evt_block_time AS block_time,
        p_bought.version,
        p_bought.token_address AS token_bought_address,
        p_sold.token_address AS token_sold_address,
        e.tokens_bought AS token_bought_amount_raw,
        e.tokens_sold AS token_sold_amount_raw,
        e.buyer AS taker,
        CAST(NULL AS varbinary) AS maker,
        e.contract_address AS project_contract_address,
        e.evt_tx_hash AS tx_hash,
        e.evt_index
    FROM {{ source('lista_lending_bnb', 'stableswappool_evt_tokenexchange') }} e
    INNER JOIN {{ ref('lista_smartswap_bnb_view_pools') }} p_bought
        ON e.contract_address = p_bought.pool_address
        AND e.bought_id = p_bought.token_id
    INNER JOIN {{ ref('lista_smartswap_bnb_view_pools') }} p_sold
        ON e.contract_address = p_sold.pool_address
        AND e.sold_id = p_sold.token_id
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('e.evt_block_time') }}
    {% endif %}
)

SELECT
    'bnb' AS blockchain,
    'lista_smartswap' AS project,
    version,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    CAST(date_trunc('day', block_time) AS date) AS block_date,
    block_time,
    block_number,
    token_bought_amount_raw,
    token_sold_amount_raw,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    evt_index
FROM dexs
