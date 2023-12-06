{{ config(
    schema = 'pancakeswap_v2_ethereum'
    , alias = 'mmpool_base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


WITH dexs AS
(
    -- PancakeSwap v2 MMPool
    SELECT
        t.evt_block_time                                                                 AS block_time
       , t.user                                                                          AS taker
       , t.mm                                                                            AS maker
       , quoteTokenAmount                                                                AS token_bought_amount_raw
       , baseTokenAmount                                                                 AS token_sold_amount_raw
       , CASE WHEN quotetoken  = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee 
             THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 ELSE quotetoken END         AS token_bought_address
       , CASE WHEN basetoken  = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee 
             THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 ELSE basetoken END          AS token_sold_address,     
       , t.contract_address                                                              AS project_contract_address
       , t.evt_tx_hash                                                                   AS tx_hash
       , t.evt_index
    FROM {{ source('pancakeswap_v2_ethereum', 'PancakeSwapMMPool_evt_Swap') }} t
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    'ethereum'                                                       AS blockchain
    , 'pancakeswap'                                             AS project
    , 'mmpool'                                                  AS version
    , CAST(date_trunc('month', dexs.block_time) AS date)        AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date)          AS block_date
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
FROM dexs