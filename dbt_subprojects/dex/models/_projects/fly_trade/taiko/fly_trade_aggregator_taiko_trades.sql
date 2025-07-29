
{% set project_start_date = '2023-01-01' %}
{% set network = 'taiko' %}
{% set hook = expose_spells('["' ~ network ~ '"]', "project", "fly_trade", '["andrew_nguyen"]') %}

{{ config
(
    schema = 'fly_trade_aggregator_' + network,
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook = hook
)
}}

WITH swaps AS (
    
    -- Version V3
    -- UNION ALL
    SELECT
        '{{ network }}' AS blockchain
        ,'fly.trade' AS project
        ,'v3' AS version
        ,CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date
        ,CAST(date_trunc('month', evt_block_time) AS DATE) AS block_month
        ,evt_block_time AS block_time
        ,amountOut AS token_bought_amount_raw
        ,amountIn AS token_sold_amount_raw
        ,toAssetAddress AS token_bought_address
        ,fromAssetAddress AS token_sold_address
        ,fromAddress AS taker
        ,CAST(NULL AS VARBINARY) AS maker
        ,contract_address AS project_contract_address	
        ,evt_tx_hash AS tx_hash
        ,evt_tx_from AS tx_from
        ,evt_tx_to AS tx_to
        ,evt_index AS evt_index
        ,ARRAY[-1] AS trace_address
    FROM
        {{ source('magpie_protocol_multichain', 'MagpieRouterV3_evt_Swap') }}
    WHERE chain = '{{ network }}'
    {% if is_incremental() %}
    AND {{incremental_predicate('evt_block_time')}}
    {% endif %}

    -- Version V3_1
    UNION ALL
    SELECT
        '{{ network }}' AS blockchain
        ,'fly.trade' AS project
        ,'v31' AS version
        ,CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date
        ,CAST(date_trunc('month', evt_block_time) AS DATE) AS block_month
        ,evt_block_time AS block_time
        ,amountOut AS token_bought_amount_raw
        ,amountIn AS token_sold_amount_raw
        ,toAssetAddress AS token_bought_address
        ,fromAssetAddress AS token_sold_address
        ,fromAddress AS taker
        ,CAST(NULL AS VARBINARY) AS maker
        ,contract_address AS project_contract_address	
        ,evt_tx_hash AS tx_hash
        ,evt_tx_from AS tx_from
        ,evt_tx_to AS tx_to
        ,evt_index AS evt_index
        ,ARRAY[-1] AS trace_address
    FROM
        {{ source('magpie_protocol_multichain', 'MagpieRouterV3_1_evt_Swap') }}
    WHERE chain = '{{ network }}'
    {% if is_incremental() %}
    AND {{incremental_predicate('evt_block_time')}}
    {% endif %}

)
SELECT
    blockchain AS blockchain
    ,project AS project
    ,version AS version
    ,block_date AS block_date
    ,block_month AS block_month
    ,swaps.block_time AS block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol)
        THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END AS token_pair
    ,swaps.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,swaps.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,CAST(swaps.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw
    ,CAST(swaps.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw
    ,COALESCE(
        (swaps.token_bought_amount_raw / power(10, (CASE swaps.token_bought_address WHEN 0x0000000000000000000000000000000000000000 THEN 18 ELSE p_bought.decimals END))) * (CASE swaps.token_bought_address WHEN 0x0000000000000000000000000000000000000000 THEN  p_eth.price ELSE p_bought.price END)
        ,(swaps.token_sold_amount_raw / power(10, (CASE swaps.token_sold_address WHEN 0x0000000000000000000000000000000000000000 THEN 18 ELSE p_sold.decimals END))) * (CASE swaps.token_sold_address WHEN 0x0000000000000000000000000000000000000000 THEN  p_eth.price ELSE p_sold.price END)
    ) AS amount_usd
    ,swaps.token_bought_address AS token_bought_address
    ,swaps.token_sold_address AS token_sold_address
    ,swaps.taker AS taker
    ,swaps.taker.maker AS maker
    ,swaps.taker.project_contract_address AS project_contract_address
    ,swaps.taker.tx_hash AS tx_hash
    ,swaps.tx_from AS tx_from
    ,swaps.tx_to AS tx_to
    ,swaps.evt_index AS evt_index
    ,swaps.trace_address AS trace_address
FROM swaps
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = swaps.token_bought_address
    AND erc20a.blockchain = '{{ network }}'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = swaps.token_sold_address
    AND erc20b.blockchain = '{{ network }}'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', swaps.block_time)
    AND p_bought.contract_address = swaps.token_bought_address
    AND p_bought.blockchain = '{{ network }}'
    {% if is_incremental() %}
    AND {{incremental_predicate('p_bought.minute')}}
    {% else %}
    AND p_bought.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', swaps.block_time)
    AND p_sold.contract_address = swaps.token_sold_address
    AND p_sold.blockchain = '{{ network }}'
    {% if is_incremental() %}
    AND {{incremental_predicate('p_sold.minute')}}
    {% else %}
    AND p_sold.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_eth
    ON p_eth.minute = date_trunc('minute', swaps.block_time)
    AND p_eth.blockchain IS NULL
    AND p_eth.symbol = 'ETH'
    {% if is_incremental() %}
    AND {{incremental_predicate('p_eth.minute')}}
    {% else %}
    AND p_eth.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
