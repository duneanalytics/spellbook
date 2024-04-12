
{{ config(
    
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "airswap",
                                    \'["jeff-dude", "hosuke", "soispoke"]\') }}'
    )
}}

{% set project_start_date = '2019-12-20' %}

WITH dexs AS
(
    SELECT
        evt_block_time AS block_time,
        'light' AS version,
        e.senderWallet AS taker,
        e.signerWallet AS maker,
        e.senderAmount AS token_sold_amount_raw,
        e.signerAmount AS token_bought_amount_raw,
        e.senderToken AS token_sold_address,
        e.signerToken AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        NULL AS amount_usd,
        evt_index
    FROM {{ source('airswap_ethereum', 'Light_evt_Swap')}} e
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL

    SELECT
        evt_block_time AS block_time,
        'light_v0' AS version,
        e.senderWallet AS taker,
        e.signerWallet AS maker,
        e.senderAmount AS token_sold_amount_raw,
        e.signerAmount AS token_bought_amount_raw,
        e.senderToken AS token_sold_address,
        e.signerToken AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        NULL AS amount_usd,
        evt_index
    FROM {{ source('airswap_ethereum', 'Light_v0_evt_Swap')}} e
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL
    
    SELECT
        evt_block_time AS block_time,
        'swap' AS version,
        e.senderWallet AS taker,
        e.signerWallet AS maker,
        e.senderAmount AS token_sold_amount_raw,
        e.signerAmount AS token_bought_amount_raw,
        e.senderToken AS token_sold_address,
        e.signerToken AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        NULL AS amount_usd,
        evt_index
    FROM {{ source('airswap_ethereum', 'swap_evt_Swap') }} e
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL

    SELECT
        evt_block_time AS block_time,
        'swap_v3' AS version,
        e.senderWallet AS taker,
        e.signerWallet AS maker,
        e.senderAmount AS token_sold_amount_raw,
        e.signerAmount AS token_bought_amount_raw,
        e.senderToken AS token_sold_address,
        e.signerToken AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        NULL AS amount_usd,
        evt_index
    FROM {{ source('airswap_ethereum', 'Swap_v3_evt_Swap')}} e
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL

    SELECT
        evt_block_time AS block_time,
        'swap_erc20_v4' AS version,
        e.senderWallet AS taker,
        e.signerWallet AS maker,
        e.senderAmount AS token_sold_amount_raw,
        e.signerAmount AS token_bought_amount_raw,
        e.senderToken AS token_sold_address,
        e.signerToken AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        NULL AS amount_usd,
        evt_index
    FROM {{ source('airswap_ethereum', 'SwapERC20_v4_evt_SwapERC20')}} e
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
SELECT
    'ethereum' AS blockchain
    ,'airswap' AS project
    ,version
    ,TRY_CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    ,CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,dexs.token_bought_amount_raw  AS token_bought_amount_raw
    ,dexs.token_sold_amount_raw  AS token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx."from") AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx."from" AS tx_from
    ,tx."to" AS tx_to
    ,dexs.evt_index
FROM dexs
INNER JOIN {{ source('ethereum', 'transactions') }} tx
    ON dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >=  TIMESTAMP '{{project_start_date}}'
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
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    AND p_bought.blockchain = 'ethereum'
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    {% if not is_incremental() %}
    AND p_sold.minute >=  TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    AND p_sold.blockchain = 'ethereum'