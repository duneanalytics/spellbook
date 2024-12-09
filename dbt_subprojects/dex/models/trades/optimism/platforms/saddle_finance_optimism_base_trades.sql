WITH dexs AS (
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        evt_tx_to AS maker,
        evt_tx_from AS taker,
        tokensBought AS token_bought_amount_raw,
        tokensSold AS token_sold_amount_raw,
        boughtId AS token_bought_address,
        soldId AS token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('saddle_finance_optimism', 'SwapFlashLoan_evt_TokenSwap') }} t
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)
-- //compiled code added
SELECT  
    'optimism' AS blockchain,
    'saddle_finance' AS project,
    '1' AS version,
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
