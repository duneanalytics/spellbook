{{ config(
    schema = 'katana_v2_ronin'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS
(
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t._to AS taker
        , t.contract_address AS maker
        , CASE WHEN _amount0Out = UINT256 '0' THEN _amount1Out ELSE _amount0Out END AS token_bought_amount_raw
        , CASE WHEN _amount0In = UINT256 '0' OR _amount1Out = UINT256 '0' THEN _amount1In ELSE _amount0In END AS token_sold_amount_raw
        , CASE WHEN _amount0Out = UINT256 '0' THEN f._token1 ELSE f._token0 END AS token_bought_address
        , CASE WHEN _amount0In = UINT256 '0' OR _amount1Out = UINT256 '0' THEN f._token1 ELSE f._token0 END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
    FROM
        {{ source('katana_dex_ronin', 'KatanaPair_evt_Swap') }} t
    INNER JOIN
        {{ source('katana_dex_ronin', 'KatanaFactory_evt_PairCreated') }} f
        ON f._pair = t.contract_address
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
    
)

SELECT
    'ronin' AS blockchain
    , 'katana' AS project
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