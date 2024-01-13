{{ config(
    schema = 'tokenlon_v5_ethereum',
    
    alias = 'amm_v2_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash'],
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "tokenlon",
                                    \'["izayl"]\') }}'
    )
}} 
{% set blockchain = 'ethereum' %}
{% set project = 'tokenlon' %}
{% set project_version = '5' %}
-- block 12792417
{% set project_start_date = "timestamp '2021-07-09'" %}

WITH dexs AS (
    SELECT 
        evt_block_time                                                             AS block_time,
        from_hex(JSON_EXTRACT_SCALAR(t."order", '$.userAddr'))                     AS taker,
        from_hex(JSON_EXTRACT_SCALAR(t."order", '$.makerAddr'))                    AS maker,
        CAST(JSON_EXTRACT_SCALAR(t."order", '$.takerAssetAmount') AS DOUBLE)       AS token_sold_amount_raw,
        CAST(JSON_EXTRACT_SCALAR(t."order", '$.makerAssetAmount') AS DOUBLE)       AS token_bought_amount_raw,
        CAST(NULL AS DOUBLE)                                                         AS amount_usd,
        CASE
            WHEN from_hex(JSON_EXTRACT_SCALAR(t."order", '$.takerAssetAddr')) IN (0x0000000000000000000000000000000000000000)
                THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            ELSE from_hex(JSON_EXTRACT_SCALAR(t."order", '$.takerAssetAddr'))
        END                                                                        AS token_sold_address,
        CASE
            WHEN from_hex(JSON_EXTRACT_SCALAR(t."order", '$.makerAssetAddr')) IN (0x0000000000000000000000000000000000000000)
                THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            ELSE from_hex(JSON_EXTRACT_SCALAR(t."order", '$.makerAssetAddr'))
        END                                                                        AS token_bought_address,
        contract_address      AS project_contract_address,
        evt_tx_hash           AS tx_hash,
        CAST(ARRAY[] as array<bigint>) AS trace_address,
        evt_index
    FROM
        {{ source('tokenlon_v5_ethereum', 'AMMWrapperWithPath_evt_Swapped') }} t
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day) 
    {% endif %}
), prices AS (
    SELECT DISTINCT
      DATE_TRUNC('hour', minute) AS hour,
      contract_address,
      blockchain,
      decimals,
      AVG(price) AS price
    FROM {{ source('prices', 'usd') }}
    GROUP BY DATE_TRUNC('hour', minute), contract_address,blockchain,decimals
)

SELECT
    '{{blockchain}}'                                            AS blockchain,
    '{{project}}'                                               AS project,
    '{{project_version}}'                                       AS version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) AS date)        AS block_date,
    CAST(date_trunc('month', dexs.block_time) AS date)          AS block_month,
    dexs.block_time,
    erc20a.symbol                                               AS token_bought_symbol,
    erc20b.symbol                                               AS token_sold_symbol,
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END                                                         AS token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals)   AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals)     AS token_sold_amount,
    CAST(dexs.token_bought_amount_raw AS UINT256)        AS token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw AS UINT256)          AS token_sold_amount_raw,
    coalesce(dexs.
        amount_usd, 
        (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price, 
        (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    )                                                           AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    coalesce(dexs.taker, tx."from")                               AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('ethereum', 'transactions') }} tx 
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= {{project_start_date}}
    {% endif %} 
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day) 
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a 
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'ethereum'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b 
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'ethereum'
LEFT JOIN prices p_bought
    ON p_bought.hour = date_trunc('hour', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'ethereum' 
    {% if not is_incremental() %}
    AND p_bought.hour >= {{project_start_date}}
    {% endif %} 
    {% if is_incremental() %}
    AND p_bought.hour >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN prices p_sold
    ON p_sold.hour = date_trunc('hour', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'ethereum' 
    {% if not is_incremental() %}
    AND p_sold.hour >= {{project_start_date}}
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.hour >= date_trunc('day', now() - interval '7' day)
    {% endif %}