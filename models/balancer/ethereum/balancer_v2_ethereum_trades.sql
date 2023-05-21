{{ config(
    schema='balancer_v2_ethereum',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2020-03-13' %}

WITH v2 AS (
    SELECT
        '2' AS version,
        tokenout AS token_bought_address,
        amountout AS token_bought_amount_raw,
        tokenin AS token_sold_address,
        amountin AS token_sold_amount_raw,
        pooladdress AS project_contract_address,
        s.evt_block_time,
        s.evt_tx_hash,
        s.evt_index
    from {{ source('balancer_v2_ethereum', 'Vault_evt_Swap') }} s
    inner join {{ source('balancer_v2_ethereum', 'Vault_evt_PoolRegistered') }} p
    on s.poolId = p.poolId
    WHERE tokenIn != poolAddress
        AND tokenOut != poolAddress
    {% if not is_incremental() %}
        AND s.evt_block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
        AND s.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),

prices AS (
    SELECT * FROM {{ source('prices', 'usd') }}
    WHERE
        blockchain = 'ethereum'
        {% if not is_incremental() %}
        and minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND minute >= DATE_TRUNC('day', NOW() - INTERVAL '1 week')
        {% endif %}
)


SELECT
    'ethereum' AS blockchain,
    'balancer' AS project,
    version,
    evt_block_time AS block_time,
    DATE_TRUNC('day', evt_block_time) AS block_date,
    erc20a.symbol AS token_bought_symbol,
    erc20b.symbol AS token_sold_symbol,
    CASE
        WHEN LOWER(erc20a.symbol) > LOWER(erc20b.symbol) THEN CONCAT(erc20b.symbol, '-', erc20a.symbol)
        ELSE CONCAT(erc20a.symbol, '-', erc20b.symbol)
    END AS token_pair,
    token_bought_amount_raw / POWER(10, erc20a.decimals) AS token_bought_amount,
    token_sold_amount_raw / POWER(10, erc20b.decimals) AS token_sold_amount,
    CAST(token_bought_amount_raw AS DECIMAL(38, 0)) AS token_bought_amount_raw,
    CAST(token_sold_amount_raw AS DECIMAL(38, 0)) AS token_sold_amount_raw,
    COALESCE(
        (token_bought_amount_raw / POWER(10, p_bought.decimals)) * p_bought.price,
        (token_sold_amount_raw / POWER(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    token_bought_address,
    token_sold_address,
    tx.from AS taker,
    CAST(NULL AS VARCHAR(5)) AS maker,
    project_contract_address,
    evt_tx_hash AS tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    evt_index,
    '' AS trace_address
FROM v2 AS trades
INNER JOIN {{ source('ethereum', 'transactions') }} AS tx
    ON
        trades.evt_tx_hash = tx.hash
        {% if not is_incremental() %}
    and tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            AND tx.block_time >= DATE_TRUNC('day', NOW() - INTERVAL '1 week')
        {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} AS erc20a
    ON
        trades.token_bought_address = erc20a.contract_address
        AND erc20a.blockchain = 'ethereum'
LEFT JOIN {{ ref('tokens_erc20') }} AS erc20b
    ON
        trades.token_sold_address = erc20b.contract_address
        AND erc20b.blockchain = 'ethereum'
LEFT JOIN prices AS p_bought
    ON
        p_bought.minute = DATE_TRUNC('minute', trades.evt_block_time)
        AND p_bought.contract_address = trades.token_bought_address
LEFT JOIN prices AS p_sold
    ON
        p_sold.minute = DATE_TRUNC('minute', trades.evt_block_time)
        AND p_sold.contract_address = trades.token_sold_address
