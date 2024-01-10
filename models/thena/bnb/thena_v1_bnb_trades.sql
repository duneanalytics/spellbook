{{ config(
    
    schema = 'thena_v1_bnb',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["bnb"]\',
        "project",
        "thena_v1",
        \'["hsrvc"]\') }}'
    )
}}

{% set project_start_date = '2023-01-01' %}

WITH dexs AS
(
    SELECT
        t.evt_block_time                                                                                AS block_time,
        t.to                                                                                            AS taker,
        CAST(NULL AS VARBINARY)                                                                         AS maker,
        CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END                          AS token_bought_amount_raw,
        CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw,
        NULL                                                                                            AS amount_usd,
        CASE WHEN amount0Out = UINT256 '0' THEN f.token1 ELSE f.token0 END                              AS token_bought_address,
        CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN f.token1 ELSE f.token0 END   AS token_sold_address,
        t.contract_address                                                                              AS project_contract_address,
        t.evt_tx_hash                                                                                   AS tx_hash,
        t.evt_index
    FROM
        {{ source('thena_fi_bnb', 'pair_evt_swap') }} t
    INNER JOIN {{ source('thena_fi_bnb', 'PairFactoryUpgradeable_evt_PairCreated') }} f
    ON t.contract_address = f.pair
    {% if is_incremental() %}
    AND t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

SELECT
    'bnb'                                                        AS blockchain
     , 'thena'                                                   AS project
     , '1'                                                       AS version
     , CAST(date_trunc('DAY', dexs.block_time) AS date)      AS block_date
     , CAST(date_trunc('month', dexs.block_time) AS date)        AS block_month
     , dexs.block_time
     , bep20a.symbol                                             AS token_bought_symbol
     , bep20b.symbol                                             AS token_sold_symbol
     , case
           when lower(bep20a.symbol) > lower(bep20b.symbol) then concat(bep20b.symbol, '-', bep20a.symbol)
           else concat(bep20a.symbol, '-', bep20b.symbol)
       end                                                       AS token_pair
     , dexs.token_bought_amount_raw / power(10, bep20a.decimals) AS token_bought_amount
     , dexs.token_sold_amount_raw / power(10, bep20b.decimals)   AS token_sold_amount
     , dexs.token_bought_amount_raw AS token_bought_amount_raw
     , dexs.token_sold_amount_raw AS token_sold_amount_raw
     , coalesce(
        dexs.amount_usd
        , (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        , (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
        )                                                        AS amount_usd
     , dexs.token_bought_address
     , dexs.token_sold_address
     , coalesce(dexs.taker, tx."from")                           AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
     , dexs.maker
     , dexs.project_contract_address
     , dexs.tx_hash
     , tx."from"                                                 AS tx_from
     , tx.to                                                     AS tx_to
     , dexs.evt_index
FROM dexs
INNER JOIN {{ source('bnb', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} bep20a
    ON bep20a.contract_address = dexs.token_bought_address
    AND bep20a.blockchain = 'bnb'
LEFT JOIN {{ source('tokens', 'erc20') }} bep20b
    ON bep20b.contract_address = dexs.token_sold_address
    AND bep20b.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
