{{ config
(
    tags=['dunesql'],
    alias = alias('aggregator_trades'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "kyberswap",
                                    \'["nhd98z"]\') }}'
)
}}

{% set project_start_date = '2023-01-01' %}

WITH meta_router AS
(
        SELECT
            evt_block_time          AS block_time
            ,'kyberswap'            AS project
            ,'0'                    AS version
            ,sender                 AS taker
            ,dstReceiver            AS maker
            ,spentAmount            AS token_bought_amount_raw
            ,returnAmount           AS token_sold_amount_raw
            ,CAST(NULL AS DOUBLE)   AS amount_usd
            ,srcToken               AS token_bought_address
            ,dstToken               AS token_sold_address
            ,contract_address       AS project_contract_address
            ,evt_tx_hash            AS tx_hash
            ,evt_index              AS evt_index
        FROM
            {{ source('kyber_arbitrum','MetaAggregationRouterV2_evt_Swapped')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)
SELECT
    'arbitrum'                                                          AS blockchain
    ,project                                                            AS project
    ,meta_router.version                                                AS version
    ,try_cast(date_trunc('day', meta_router.block_time) AS date)        AS block_date
    ,meta_router.block_time                                             AS block_time
    ,erc20a.symbol                                                      AS token_bought_symbol
    ,erc20b.symbol                                                      AS token_sold_symbol
    ,CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol)
        THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
        END                                                             AS token_pair
    ,meta_router.token_bought_amount_raw / power(10, erc20a.decimals)   AS token_bought_amount
    ,meta_router.token_sold_amount_raw / power(10, erc20b.decimals)     AS token_sold_amount
    ,meta_router.token_bought_amount_raw                                AS token_bought_amount_raw
    ,meta_router.token_sold_amount_raw                                  AS token_sold_amount_raw
    ,coalesce(
        meta_router.amount_usd
        ,(meta_router.token_bought_amount_raw / power(10, (CASE meta_router.token_bought_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE p_bought.decimals END))) * (CASE meta_router.token_bought_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN  p_eth.price ELSE p_bought.price END)
        ,(meta_router.token_sold_amount_raw / power(10, (CASE meta_router.token_sold_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE p_sold.decimals END))) * (CASE meta_router.token_sold_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN  p_eth.price ELSE p_sold.price END)
        )                                                               AS amount_usd
    ,meta_router.token_bought_address                                   AS token_bought_address
    ,meta_router.token_sold_address                                     AS token_sold_address
    ,coalesce(meta_router.taker, tx."from")                             AS taker
    ,meta_router.maker                                                  AS maker
    ,meta_router.project_contract_address                               AS project_contract_address
    ,meta_router.tx_hash                                                AS tx_hash
    ,tx."from"                                                          AS tx_from
    ,tx."to"                                                            AS tx_to
    ,meta_router.evt_index                                              AS evt_index
FROM meta_router
INNER JOIN {{ source('arbitrum', 'transactions')}} tx
    ON meta_router.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} erc20a
    ON erc20a.contract_address = meta_router.token_bought_address
    AND erc20a.blockchain = 'arbitrum'
LEFT JOIN {{ ref('tokens_erc20') }} erc20b
    ON erc20b.contract_address = meta_router.token_sold_address
    AND erc20b.blockchain = 'arbitrum'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', meta_router.block_time)
    AND CAST(p_bought.contract_address AS VARBINARY) = meta_router.token_bought_address
    AND p_bought.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', meta_router.block_time)
    AND CAST(p_sold.contract_address AS VARBINARY) = meta_router.token_sold_address
    AND p_sold.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_eth
    ON p_eth.minute = date_trunc('minute', meta_router.block_time)
    AND p_eth.blockchain is null
    AND p_eth.symbol = 'ETH'
    {% if not is_incremental() %}
    AND p_eth.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    AND p_eth.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
WHERE CAST(meta_router.token_bought_address AS VARBINARY) <> CAST(meta_router.token_sold_address AS VARBINARY)
;
