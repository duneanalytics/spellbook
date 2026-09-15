{{ config
(
    schema = 'kyberswap_aggregator_robinhood',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)
}}

{% set project_start_date = '2026-07-12' %}

WITH meta_router AS
(
        SELECT
            block_time              AS block_time
            ,'kyberswap'            AS project
            ,'meta_2'               AS version
            ,bytearray_substring(data, 13, 20)  AS taker
            ,bytearray_substring(data, 109, 20) AS maker
            ,bytearray_to_uint256(bytearray_substring(data, 161, 32)) AS token_bought_amount_raw
            ,bytearray_to_uint256(bytearray_substring(data, 129, 32)) AS token_sold_amount_raw
            ,CAST(NULL AS DOUBLE)   AS amount_usd
            ,bytearray_substring(data, 77, 20)  AS token_bought_address
            ,bytearray_substring(data, 45, 20)  AS token_sold_address
            ,contract_address       AS project_contract_address
            ,tx_hash                AS tx_hash
            ,index                  AS evt_index
            ,ARRAY[-1]              AS trace_address
        FROM
            {{ source('robinhood', 'logs') }}
        WHERE
            contract_address = 0x6131b5fae19ea4f9d964eac0408e4408b66337b5
            AND topic0 = 0xd6d4f5681c246c9f42c203e287975af1601f8df8035a9251f79aab5c8f09e2f8
            AND block_date >= DATE '{{ project_start_date }}'
            {% if is_incremental() %}
            AND {{incremental_predicate('block_time')}}
            {% endif %}
)
SELECT
    'robinhood'                                                     AS blockchain
    ,project                                                            AS project
    ,meta_router.version                                                AS version
    ,CAST(date_trunc('day', meta_router.block_time) AS DATE)            AS block_date
    ,CAST(date_trunc('month', meta_router.block_time) AS DATE)          AS block_month
    ,meta_router.block_time                                             AS block_time
    ,erc20a.symbol                                                      AS token_bought_symbol
    ,erc20b.symbol                                                      AS token_sold_symbol
    ,CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol)
        THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
        END                                                             AS token_pair
    ,meta_router.token_bought_amount_raw / power(10, CASE meta_router.token_bought_address WHEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 18 ELSE erc20a.decimals END) AS token_bought_amount
    ,meta_router.token_sold_amount_raw / power(10, CASE meta_router.token_sold_address WHEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 18 ELSE erc20b.decimals END) AS token_sold_amount
    ,CAST(meta_router.token_bought_amount_raw AS UINT256)               AS token_bought_amount_raw
    ,CAST(meta_router.token_sold_amount_raw AS UINT256)                 AS token_sold_amount_raw
    ,COALESCE(
        meta_router.amount_usd
        ,(meta_router.token_bought_amount_raw / power(10, (CASE meta_router.token_bought_address WHEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 18 ELSE p_bought.decimals END))) * (CASE meta_router.token_bought_address WHEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN  p_eth.price ELSE p_bought.price END)
        ,(meta_router.token_sold_amount_raw / power(10, (CASE meta_router.token_sold_address WHEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 18 ELSE p_sold.decimals END))) * (CASE meta_router.token_sold_address WHEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN  p_eth.price ELSE p_sold.price END)
        )                                                               AS amount_usd
    ,meta_router.token_bought_address                                   AS token_bought_address
    ,meta_router.token_sold_address                                     AS token_sold_address
    ,COALESCE(meta_router.taker, tx."from")                             AS taker
    ,meta_router.maker                                                  AS maker
    ,meta_router.project_contract_address                               AS project_contract_address
    ,meta_router.tx_hash                                                AS tx_hash
    ,tx."from"                                                          AS tx_from
    ,tx."to"                                                            AS tx_to
    ,meta_router.evt_index                                              AS evt_index
    ,meta_router.trace_address                                          AS trace_address
FROM meta_router
INNER JOIN {{ source('robinhood', 'transactions')}} tx
    ON meta_router.tx_hash = tx.hash
    {% if is_incremental() %}
    AND {{incremental_predicate('tx.block_time')}}
    {% else %}
    AND tx.block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = meta_router.token_bought_address
    AND erc20a.blockchain = 'robinhood'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = meta_router.token_sold_address
    AND erc20b.blockchain = 'robinhood'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', meta_router.block_time)
    AND p_bought.contract_address = meta_router.token_bought_address
    AND p_bought.blockchain = 'robinhood'
    {% if is_incremental() %}
    AND {{incremental_predicate('p_bought.minute')}}
    {% else %}
    AND p_bought.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', meta_router.block_time)
    AND p_sold.contract_address = meta_router.token_sold_address
    AND p_sold.blockchain = 'robinhood'
    {% if is_incremental() %}
    AND {{incremental_predicate('p_sold.minute')}}
    {% else %}
    AND p_sold.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_eth
    ON p_eth.minute = date_trunc('minute', meta_router.block_time)
    AND p_eth.blockchain IS NULL
    AND p_eth.symbol = 'ETH'
    {% if is_incremental() %}
    AND {{incremental_predicate('p_eth.minute')}}
    {% else %}
    AND p_eth.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
