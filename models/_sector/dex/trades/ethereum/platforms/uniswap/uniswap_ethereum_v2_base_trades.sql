{{ config(
    schema = 'uniswap_ethereum',
    alias = 'v2_base_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2020-05-05' %}
{% set weth_ubomb_wash_trading_pair = '0xed9c854cb02de75ce4c9bba992828d6cb7fd5c71' %}
{% set weth_weth_wash_trading_pair = '0xf9c1fa7d41bf44ade1dd08d37cc68f67ae75bf92' %}
{% set feg_eth_wash_trading_pair = '0x854373387e41371ac6e307a1f29603c6fa10d872' %}

WITH dexs AS
(
    -- Uniswap v2
    SELECT t.evt_block_time                                                            AS block_time
         , t.to                                                                        AS taker
         , ''                                                                          AS maker
         , CASE WHEN amount0Out = 0 THEN amount1Out ELSE amount0Out END                AS token_bought_amount_raw
         , CASE WHEN amount0In = 0 OR amount1Out = 0 THEN amount1In ELSE amount0In END AS token_sold_amount_raw
         , CAST(NULL AS DOUBLE)                                                        AS amount_usd
         , CASE WHEN amount0Out = 0 THEN f.token1 ELSE f.token0 END                    AS token_bought_address
         , CASE WHEN amount0In = 0 OR amount1Out = 0 THEN f.token1 ELSE f.token0 END   AS token_sold_address
         , t.contract_address                                                          AS project_contract_address
         , t.evt_tx_hash                                                               AS tx_hash
         , ''                                                                          AS trace_address
         , t.evt_index
    FROM {{ source('uniswap_v2_ethereum', 'Pair_evt_Swap') }} t
    INNER JOIN {{ source('uniswap_v2_ethereum', 'Factory_evt_PairCreated') }} f
        ON f.pair = t.contract_address
    WHERE t.contract_address NOT IN (
        '{{weth_ubomb_wash_trading_pair}}',
        '{{weth_weth_wash_trading_pair}}',
        '{{feg_eth_wash_trading_pair}}' )
    {% if is_incremental() %}
    AND t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
     , dexs.block_time
     , CAST(dexs.token_bought_amount_raw AS DECIMAL(38, 0)) AS token_bought_amount_raw
     , CAST(dexs.token_sold_amount_raw AS DECIMAL(38, 0))   AS token_sold_amount_raw
     , dexs.amount_usd
     , dexs.token_bought_address
     , dexs.token_sold_address
     , dexs.taker
     , dexs.maker
     , dexs.project_contract_address
     , dexs.tx_hash
     , dexs.trace_address
     , dexs.evt_index
FROM dexs
;