{{ config(
    alias = alias('trades'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "kyberswap",
                                \'["zhongyiio", "hosuke", "msilb7"]\') }}'
    )
}}

{% set project_start_date = '2021-08-31' %}

WITH

kyberswap_dex AS (
    SELECT
        t.evt_block_time                                                    AS block_time
        ,t.`to`                                                             AS taker
        ,''                                                                 AS maker
        ,CASE WHEN t.amount0Out = 0 THEN t.amount1Out ELSE t.amount0Out END AS token_bought_amount_raw
        ,CASE WHEN t.amount0In = 0 THEN t.amount1In ELSE t.amount0In END    AS token_sold_amount_raw
        ,cast(NULL as double)                                               AS amount_usd
        ,CASE WHEN t.amount0Out = 0 THEN p.token1 ELSE p.token0 END         AS token_bought_address
        ,CASE WHEN t.amount0In = 0 THEN p.token1 ELSE p.token0 END          AS token_sold_address
        ,t.contract_address                                                 AS project_contract_address
        ,t.evt_tx_hash                                                      AS tx_hash
        ,''                                                                 AS trace_address
        ,t.evt_index

    FROM {{ source('kyber_optimism', 'DMM_Pool_evt_Swap') }} t
    INNER JOIN {{ source('kyberswap_optimism', 'DMM_Factory_evt_PoolCreated') }} p
        ON t.contract_address = p.pool
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}

    UNION ALL

    -- https://docs.kyberswap.com/contract/implement-a-swap
    -- deltaQty0 and deltaQty1, Negative numbers represent the sold amount, and positive numbers represent the buy amount
    SELECT
        t.evt_block_time                                                               AS block_time
        ,t.sender                                                                      AS taker
        ,t.recipient                                                                   AS maker
        ,if(startswith(t.deltaQty0, '-'), t.deltaQty1, t.deltaQty0)                    AS token_bought_amount_raw
        ,replace(if(startswith(t.deltaQty0, '-'), t.deltaQty0, t.deltaQty1), '-', '')  AS token_sold_amount_raw
        ,cast(NULL as double)                                                          AS amount_usd
        ,if(startswith(t.deltaQty0, '-'), p.token1, p.token0)                          AS token_bought_address
        ,if(startswith(t.deltaQty0, '-'), p.token0, p.token1)                          AS token_sold_address
        ,t.contract_address                                                            AS project_contract_address
        ,t.evt_tx_hash                                                                 AS tx_hash
        ,''                                                                            AS trace_address
        ,t.evt_index

    FROM {{ source('kyber_optimism', 'Elastic_Pool_evt_swap') }} t
    INNER JOIN {{ source('kyber_optimism', 'Elastic_Factory_evt_PoolCreated') }} p
        ON t.contract_address = p.pool
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}
    
    UNION ALL

    SELECT
        evt_block_time                                                     AS block_time
        ,sender                                                            AS taker
        ,''                                                                AS maker
        ,returnAmount                                                      AS token_bought_amount_raw
        ,spentAmount                                                       AS token_sold_amount_raw
        ,cast(NULL as double)                                              AS amount_usd
        ,dstToken                                                          AS token_bought_address
        ,srcToken                                                          AS token_sold_address
        ,contract_address                                                  AS project_contract_address
        ,evt_tx_hash                                                       AS tx_hash
        ,''                                                                AS trace_address
        ,evt_index

    FROM {{ source('kyber_optimism', 'AggregationRouterV3_evt_Swapped') }}
    WHERE
        {% if is_incremental() %}
        evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        evt_block_time >= '{{ project_start_date }}'
        {% endif %}

    UNION ALL

    SELECT
        evt_block_time                                                     AS block_time
        ,sender                                                            AS taker
        ,''                                                                AS maker
        ,returnAmount                                                      AS token_bought_amount_raw
        ,spentAmount                                                       AS token_sold_amount_raw
        ,cast(NULL as double)                                              AS amount_usd
        ,dstToken                                                          AS token_bought_address
        ,srcToken                                                          AS token_sold_address
        ,contract_address                                                  AS project_contract_address
        ,evt_tx_hash                                                       AS tx_hash
        ,''                                                                AS trace_address
        ,evt_index

    FROM {{ source('kyber_optimism', 'MetaAggregationRouter_evt_Swapped') }}
    WHERE
        {% if is_incremental() %}
        evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        evt_block_time >= '{{ project_start_date }}'
        {% endif %}
    
    UNION ALL

    SELECT
        evt_block_time                                                     AS block_time
        ,sender                                                            AS taker
        ,''                                                                AS maker
        ,returnAmount                                                      AS token_bought_amount_raw
        ,spentAmount                                                       AS token_sold_amount_raw
        ,cast(NULL as double)                                              AS amount_usd
        ,dstToken                                                          AS token_bought_address
        ,srcToken                                                          AS token_sold_address
        ,contract_address                                                  AS project_contract_address
        ,evt_tx_hash                                                       AS tx_hash
        ,''                                                                AS trace_address
        ,evt_index
        
    FROM {{ source('kyberswap_optimism', 'MetaAggregationRouter_evt_Swapped') }}
    WHERE
        {% if is_incremental() %}
        evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        evt_block_time >= '{{ project_start_date }}'
        {% endif %}
)

SELECT
    'optimism'                                                         AS blockchain
    ,'kyberswap'                                                          AS project
    ,'dmm'                                                                AS version
    ,try_cast(date_trunc('DAY', kyberswap_dex.block_time) AS date)        AS block_date
    ,kyberswap_dex.block_time
    ,erc20a.symbol                                                        AS token_bought_symbol
    ,erc20b.symbol                                                        AS token_sold_symbol
    ,CASE
         WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
         ELSE concat(erc20a.symbol, '-', erc20b.symbol)
     END                                                                  AS token_pair
    ,kyberswap_dex.token_bought_amount_raw / power(10, erc20a.decimals)   AS token_bought_amount
    ,kyberswap_dex.token_sold_amount_raw / power(10, erc20b.decimals)     AS token_sold_amount
    ,CAST(kyberswap_dex.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw
    ,CAST(kyberswap_dex.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw
    ,coalesce(kyberswap_dex.amount_usd
            ,(kyberswap_dex.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
            ,(kyberswap_dex.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
     )                                                                   AS amount_usd
    ,kyberswap_dex.token_bought_address
    ,kyberswap_dex.token_sold_address
    ,coalesce(kyberswap_dex.taker, tx.from)                              AS taker
    ,kyberswap_dex.maker
    ,kyberswap_dex.project_contract_address
    ,kyberswap_dex.tx_hash
    ,tx.from                                                             AS tx_from
    ,tx.to                                                               AS tx_to
    ,kyberswap_dex.trace_address
    ,kyberswap_dex.evt_index
FROM kyberswap_dex
INNER JOIN {{ source('optimism', 'transactions') }} tx
    ON kyberswap_dex.tx_hash = tx.hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20_legacy') }} erc20a
    ON erc20a.contract_address = kyberswap_dex.token_bought_address
    and erc20a.blockchain = 'optimism'
LEFT JOIN {{ ref('tokens_erc20_legacy') }} erc20b
    ON erc20b.contract_address = kyberswap_dex.token_sold_address
    AND erc20b.blockchain = 'optimism'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', kyberswap_dex.block_time)
    AND p_bought.contract_address = kyberswap_dex.token_bought_address
    AND p_bought.blockchain = 'optimism'
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', kyberswap_dex.block_time)
    AND p_sold.contract_address = kyberswap_dex.token_sold_address
    AND p_sold.blockchain = 'optimism'
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
WHERE kyberswap_dex.token_bought_address != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
    OR kyberswap_dex.token_sold_address != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
;
