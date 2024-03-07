{{
    config(
        alias = 'crosschain_trades'
        ,schema = 'hashflow_optimism'
        ,partition_by = ['block_date']
        ,materialized='incremental'
        ,incremental_strategy = 'merge'
        ,unique_key = ['block_date', 'source_chain', 'tx_hash']
        ,post_hook='{{ expose_spells(\'["optimism"]\',
                                        "project",
                                        "hashflow",
                                        \'["ARDev097"]\') }}'
    )
}}

with cross_chain_trades AS (
        SELECT
            evt_block_time           AS block_time
            ,trader                  
            ,quoteTokenAmount        AS token_bought_amount_raw
            ,baseTokenAmount         AS token_sold_amount_raw
            ,cast(NULL AS double)    AS amount_usd
            ,quoteToken              AS token_bought_address
            ,baseToken               AS token_sold_address
            ,evt_tx_hash             AS tx_hash
            ,CASE WHEN dstChainId = 1 OR dstChainId = 101 THEN 'ethereum'
                  WHEN dstChainId = 10 OR dstChainId = 110 THEN 'arbitrum'
                  WHEN dstChainId = 11 OR dstChainId = 111 THEN 'optimism'
                  WHEN dstChainId = 6 OR dstChainId = 106 THEN 'avalanche_c'
                  WHEN dstChainId = 9 OR dstChainId = 109 THEN 'polygon'
                  WHEN dstChainId = 2 OR dstChainId = 102 THEN 'bnb' END AS destination_chain
            ,'optimism'                   AS source_chain
        FROM
            {{ source('hashflow_optimism', 'Pool_evt_LzTrade') }}
        {% if is_incremental() %}
            WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        
        UNION

        SELECT
            evt_block_time           AS block_time
            ,trader            
            ,quoteTokenAmount        AS token_bought_amount_raw
            ,baseTokenAmount         AS token_sold_amount_raw
            ,cast(NULL AS double)    AS amount_usd
            ,quoteToken              AS token_bought_address
            ,baseToken               AS token_sold_address
            ,evt_tx_hash             AS tx_hash
            ,CASE WHEN dstChainId = 1 THEN 'ethereum'
                  WHEN dstChainId = 2 THEN 'arbitrum'
                  WHEN dstChainId = 3 THEN 'optimism'
                  WHEN dstChainId = 4 THEN 'avalanche_c'
                  WHEN dstChainId = 5 THEN 'polygon'
                  WHEN dstChainId = 6 THEN 'bnb' END AS destination_chain
            ,'optimism'                   AS source_chain
        FROM
            {{ source('hashflow_optimism', 'Pool_evt_XChainTrade') }}
        {% if is_incremental() %}
            WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
)

SELECT
    try_cast(date_trunc('DAY', cross_chain_trades.block_time) AS date)       AS block_date
    ,cross_chain_trades.block_time
    ,erc20a.symbol                                                           AS token_bought_symbol
    ,erc20b.symbol                                                           AS token_sold_symbol
    ,cross_chain_trades.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,cross_chain_trades.token_sold_amount_raw / power(10, erc20b.decimals)   AS token_sold_amount
    ,CAST(cross_chain_trades.token_bought_amount_raw AS DECIMAL(38,0))       AS token_bought_amount_raw
    ,CAST(cross_chain_trades.token_sold_amount_raw AS DECIMAL(38,0))         AS token_sold_amount_raw
    ,coalesce(
            cross_chain_trades.amount_usd
        , (cross_chain_trades.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        , (cross_chain_trades.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
        )                                                                   AS amount_usd
    ,cross_chain_trades.token_bought_address
    ,cross_chain_trades.token_sold_address
    ,cross_chain_trades.trader
    ,cross_chain_trades.tx_hash
    ,cross_chain_trades.source_chain
    ,cross_chain_trades.destination_chain
FROM cross_chain_trades
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = cross_chain_trades.token_bought_address
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = cross_chain_trades.token_sold_address
    AND erc20b.blockchain = cross_chain_trades.source_chain
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', cross_chain_trades.block_time)
    AND p_bought.contract_address = cross_chain_trades.token_bought_address
    AND p_bought.blockchain = 'optimism'
    {% if is_incremental() %}
        AND  {{ incremental_predicate('p_bought.minute') }}
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', cross_chain_trades.block_time)
    AND p_sold.contract_address = cross_chain_trades.token_sold_address
    AND p_sold.blockchain = 'optimism'
    {% if is_incremental() %}
        AND {{ incremental_predicate('p_sold.minute') }}
    {% endif %}
    