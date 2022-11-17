{{ config(
    schema = 'balancer_v2_arbitrum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "balancer_v2",
                                \'["mtitus6"]\') }}'
    )
}}

{% set project_start_date = '2021-08-27' %}

with dexs as (
    SELECT
        t.evt_block_time AS block_time,
        NULL AS taker, 
        NULL AS maker,
        t.amountOut AS token_bought_amount_raw,
        t.amountIn AS token_sold_amount_raw,
        NULL AS amount_usd,
        t.tokenOut AS token_bought_address,
        t.tokenIn AS token_sold_address,
        t.poolId AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        '' AS trace_address,
        t.evt_index
    FROM
        {{ source('balancer_v2_arbitrum', 'Vault_evt_Swap') }} t
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    )
SELECT 
    'balancer' AS project,
    '2' AS version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
    tx.from as taker, 
    maker,
    token_bought_amount_raw,
    token_sold_amount_raw,
    coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    token_bought_address,
    token_sold_address,
    project_contract_address,
    tx_hash,
    tx.from as tx_from,
    tx.to as tx_to,
    trace_address,
    evt_index
    FROM dexs
    INNER JOIN 
    {{ source('arbitrum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address 
    AND erc20a.blockchain = 'arbitrum'
LEFT JOIN {{ ref('tokens_erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'arbitrum'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}