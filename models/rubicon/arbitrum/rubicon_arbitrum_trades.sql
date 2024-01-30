{{ 
    config(
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ 
        expose_spells(\'["arbitrum"]\',
        "project",
        "rubicon",
        \'["denver"]\') }}'
    )
}}

{% set project_start_date = '2023-06-09' %}

WITH dexs AS
(
    -- -- useful syntax when dealing with the event LogTake
    -- -- pay_gem corresponds with take_amt - this is what the taker is taking and what the maker is selling
    -- -- buy_gem corresponds with give_amt - this is what the taker is giving and what the maker is buying

    --From the prespective of the taker
    SELECT
        t.evt_block_time AS block_time, 
        t.evt_block_number,
        t.taker AS taker,
        t.maker AS maker,
        t.take_amt AS token_bought_amount_raw,
        t.give_amt AS token_sold_amount_raw,
        NULL AS amount_usd,
        t.pay_gem AS token_bought_address,
        t.buy_gem AS token_sold_address,
        t.contract_address as project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM
        {{ source('rubicon_arbitrum', 'RubiconMarket_evt_emitTake') }} t
        
    WHERE t.evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% if is_incremental() %}
    AND t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
SELECT
    'arbitrum' AS blockchain,
    'rubicon' AS project,
    '1' AS version,
    CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    dexs.block_time,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
    dexs.token_bought_amount_raw  AS token_bought_amount_raw,
    dexs.token_sold_amount_raw  AS token_sold_amount_raw,
    coalesce(
        dexs.amount_usd,
        (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
        (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('arbitrum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    AND tx.block_number = dexs.evt_block_number
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address 
    AND erc20a.blockchain = 'arbitrum'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'arbitrum'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}