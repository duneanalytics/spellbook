{{ config(
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "kyberswap",
                                \'["ppclunghe", "gregshestakovlido"]\') }}'
    )
}}

{% set project_start_date = '2022-02-11' %}  

WITH

kyberswap_dex AS (
    SELECT
        t.evt_block_time                                                              AS block_time
        ,t."to"                                                                       AS taker
        ,CAST(NULL AS VARBINARY)                                                      AS maker
        ,CASE WHEN t.amount0Out = UINT256 '0' THEN t.amount1Out ELSE t.amount0Out END AS token_bought_amount_raw
        ,CASE WHEN t.amount0In = UINT256 '0' THEN t.amount1In ELSE t.amount0In END    AS token_sold_amount_raw
        ,NULL                                                                         AS amount_usd
        ,CASE WHEN t.amount0Out = UINT256 '0' THEN p.token1 ELSE p.token0 END         AS token_bought_address
        ,CASE WHEN t.amount0In = UINT256 '0' THEN p.token1 ELSE p.token0 END          AS token_sold_address
        ,t.contract_address                                                           AS project_contract_address
        ,t.evt_tx_hash                                                                AS tx_hash
        ,'classic'                                                                    AS version
        ,t.evt_index
    FROM {{ source('kyber_arbitrum', 'DMM_Pool_evt_Swap') }} t
    INNER JOIN {{ source('kyber_arbitrum', 'DMMFactory_evt_PoolCreated') }} p
        ON t.contract_address = p.pool
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE t.evt_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}

    UNION ALL

    -- https://docs.kyberswap.com/contract/implement-a-swap
    -- deltaQty0 and deltaQty1, Negative numbers represent the sold amount, and positive numbers represent the buy amount
    SELECT
        t.evt_block_time                                                                                          AS block_time
        ,t.sender                                                                                                 AS taker
        ,t.recipient                                                                                              AS maker
        ,cast(if(starts_with(cast(t.deltaQty0 as varchar), '-'), abs(t.deltaQty1), abs(t.deltaQty0)) as uint256)  AS token_bought_amount_raw
        ,cast(if(starts_with(cast(t.deltaQty0 as varchar), '-'), abs(t.deltaQty0), abs(t.deltaQty1)) as uint256)  AS token_sold_amount_raw
        ,NULL                                                                                                     AS amount_usd
        ,if(starts_with(cast(t.deltaQty0 as varchar), '-'), p.token1, p.token0)                                   AS token_bought_address
        ,if(starts_with(cast(t.deltaQty0 as varchar), '-'), p.token0, p.token1)                                   AS token_sold_address
        ,t.contract_address                                                                                       AS project_contract_address
        ,t.evt_tx_hash                                                                                            AS tx_hash
        ,'elastic'                                                                                                AS version	
        ,t.evt_index

    FROM {{ source('kyber_arbitrum', 'Elastic_Pool_evt_swap') }} t
    INNER JOIN {{ source('kyber_arbitrum', 'Elastic_Factory_evt_PoolCreated') }} p
        ON t.contract_address = p.pool
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE t.evt_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}

    UNION ALL

    SELECT
        t.evt_block_time                                                                                          AS block_time
        ,t.sender                                                                                                 AS taker
        ,t.recipient                                                                                              AS maker
        ,cast(if(starts_with(cast(t.deltaQty0 as varchar), '-'), abs(t.deltaQty1), abs(t.deltaQty0)) as uint256)  AS token_bought_amount_raw
        ,cast(if(starts_with(cast(t.deltaQty0 as varchar), '-'), abs(t.deltaQty0), abs(t.deltaQty1)) as uint256)  AS token_sold_amount_raw
        ,NULL                                                                                                     AS amount_usd
        ,if(starts_with(cast(t.deltaQty0 as varchar), '-'), p.token1, p.token0)                                   AS token_bought_address
        ,if(starts_with(cast(t.deltaQty0 as varchar), '-'), p.token0, p.token1)                                   AS token_sold_address
        ,t.contract_address                                                                                       AS project_contract_address
        ,t.evt_tx_hash                                                                                            AS tx_hash
        ,'elastic_2'                                                                                              AS version
        ,t.evt_index

    FROM {{ source('kyber_arbitrum', 'ElasticPoolV2_evt_Swap') }} t
    INNER JOIN {{ source('kyber_arbitrum', 'ElasticFactoryV2_evt_PoolCreated') }} p
        ON t.contract_address = p.pool
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE t.evt_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
)

SELECT 'arbitrum'                                                         AS blockchain
     , 'kyberswap'                                                        AS project
     , version                                                            AS version
     , try_cast(date_trunc('day', kyberswap_dex.block_time) AS date)      AS block_date
     , try_cast(date_trunc('month', kyberswap_dex.block_time) AS date)    AS block_month
     , kyberswap_dex.block_time
     , erc20a.symbol                                                      AS token_bought_symbol
     , erc20b.symbol                                                      AS token_sold_symbol
     , CASE
           WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
           ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END                                                                   AS token_pair
     , kyberswap_dex.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
     , kyberswap_dex.token_sold_amount_raw / power(10, erc20b.decimals)   AS token_sold_amount
     , kyberswap_dex.token_bought_amount_raw                              AS token_bought_amount_raw
     , kyberswap_dex.token_sold_amount_raw                                AS token_sold_amount_raw
     , coalesce(kyberswap_dex.amount_usd
    , (kyberswap_dex.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
    , (kyberswap_dex.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    )                                                                     AS amount_usd
     , kyberswap_dex.token_bought_address
     , kyberswap_dex.token_sold_address
     , coalesce(kyberswap_dex.taker, tx."from")                           AS taker
     , kyberswap_dex.maker
     , kyberswap_dex.project_contract_address
     , kyberswap_dex.tx_hash
     , tx."from"                                                          AS tx_from
     , tx.to                                                              AS tx_to
     , kyberswap_dex.evt_index
FROM kyberswap_dex
INNER JOIN {{ source('arbitrum', 'transactions') }} tx
    ON kyberswap_dex.tx_hash = tx.hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = kyberswap_dex.token_bought_address
    and erc20a.blockchain = 'arbitrum'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = kyberswap_dex.token_sold_address
    AND erc20b.blockchain = 'arbitrum'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', kyberswap_dex.block_time)
    AND p_bought.contract_address = kyberswap_dex.token_bought_address
    AND p_bought.blockchain = 'arbitrum'
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', kyberswap_dex.block_time)
    AND p_sold.contract_address = kyberswap_dex.token_sold_address
    AND p_sold.blockchain = 'arbitrum'
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% else %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
WHERE (kyberswap_dex.token_bought_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    OR kyberswap_dex.token_sold_address != 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)
