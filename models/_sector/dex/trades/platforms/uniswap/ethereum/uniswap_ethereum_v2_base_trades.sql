{{ config(
    tags=['dunesql'],
    schema = 'uniswap_v2_ethereum',
    alias = 'base_trades',
    partition_by = ['block_month'],
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
         , CAST(NULL as VARBINARY) as maker
         , CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
         , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
         , CASE WHEN amount0Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_bought_address
         , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
         , t.contract_address as project_contract_address
         , t.evt_tx_hash AS tx_hash
         , t.evt_index
    FROM {{ source('uniswap_v2_ethereum', 'Pair_evt_Swap') }} t
    INNER JOIN {{ source('uniswap_v2_ethereum', 'Factory_evt_PairCreated') }} f
        ON f.pair = t.contract_address
    WHERE t.contract_address NOT IN (
        {{weth_ubomb_wash_trading_pair}},
        {{weth_weth_wash_trading_pair}},
        {{feg_eth_wash_trading_pair}}
    )
    {% if is_incremental() %}
    AND {{incremental_predicate('t.evt_block_time')}}
    {% endif %}
)

SELECT 
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , dexs.block_time
    , dexs.token_bought_amount_raw  AS token_bought_amount_raw
    , dexs.token_sold_amount_raw AS token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM dexs