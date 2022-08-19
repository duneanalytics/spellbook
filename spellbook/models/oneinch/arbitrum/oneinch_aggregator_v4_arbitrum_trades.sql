{{ config(
    schema = 'oneinch_aggregator_v4_arbitrum_trades',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}
WITH aggregators AS
(
    -- 1inch Aggregator V4
    SELECT
        t.call_block_time AS block_time
        ,get_json_object(t.`desc`, '$.dstReceiver') AS taker
        ,'' AS maker
        ,t.output_returnAmount AS token_bought_amount_raw
        ,get_json_object(t.`desc`, '$.amount') AS token_sold_amount_raw
        ,NULL AS amount_usd
        ,get_json_object(t.`desc`, '$.dstToken') AS token_bought_address
        ,get_json_object(t.`desc`, '$.srcToken') AS token_sold_address
        ,t.contract_address AS project_contract_address
        ,t.call_tx_hash AS tx_hash
        ,t.call_trace_address AS trace_address
        ,NULL AS evt_index
    FROM
        {{ source('oneinch_arbitrum', 'AggregationRouterV4_call_swap') }} t
    {% if is_incremental() %}
    WHERE t.evt_block_time >= (SELECT MAX(block_time) FROM {{ this }})
    {% endif %}
)
SELECT
    'arbitrum' AS blockchain
    ,'1inch' AS project
    ,'4' AS version
    ,TRY_CAST(date_trunc('DAY', aggregators.block_time) AS date) AS block_date
    ,aggregators.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,aggregators.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,aggregators.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,aggregators.token_bought_amount_raw
    ,aggregators.token_sold_amount_raw
    ,coalesce(
        aggregators.amount_usd
        ,(aggregators.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(aggregators.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd
    ,aggregators.token_bought_address
    ,aggregators.token_sold_address
    ,coalesce(aggregators.taker, tx.from) AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,aggregators.maker
    ,aggregators.project_contract_address
    ,aggregators.tx_hash
    ,tx.from AS tx_from
    ,tx.to AS tx_to
    ,aggregators.trace_address
    ,aggregators.evt_index
    ,'1inch' ||'-'|| '1' ||'-'|| aggregators.tx_hash ||'-'|| IFNULL(aggregators.evt_index, '') ||'-'|| IFNULL(aggregators.trace_address, '') AS unique_trade_id
FROM aggregators
INNER JOIN {{ source('arbitrum', 'transactions') }} tx
    ON tx.hash = aggregators.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= (SELECT MIN(block_time) FROM aggregators)
    {% endif %}
    {% if is_incremental() %}
    AND TRY_CAST(date_trunc('DAY', tx.block_time) AS date) = TRY_CAST(date_trunc('DAY', aggregators.block_time) AS date)
    {% endif %}
LEFT JOIN {{ ref('tokens_arbitrum_erc20') }} erc20a ON erc20a.contract_address = aggregators.token_bought_address
LEFT JOIN {{ ref('tokens_arbitrum_erc20') }} erc20b ON erc20b.contract_address = aggregators.token_sold_address
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', aggregators.block_time)
    AND p_bought.contract_address = aggregators.token_bought_address
    AND p_bought.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_bought.minute >= (SELECT MIN(block_time) FROM aggregators)
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= (SELECT MAX(block_time) FROM {{ this }})
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.minute = date_trunc('minute', aggregators.block_time)
    AND p_sold.contract_address = aggregators.token_sold_address
    AND p_sold.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_sold.minute >= (SELECT MIN(block_time) FROM aggregators)
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= (SELECT MAX(block_time) FROM {{ this }})
    {% endif %}
