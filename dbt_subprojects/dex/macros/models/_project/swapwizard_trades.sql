{% macro swapwizard_trades(blockchain, contract_address) %}

{% set project_start_date = '2026-05-19' %}

{% set w_native = {
    'ethereum': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
    'bnb': '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',
    'polygon': '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270',
    'base': '0x4200000000000000000000000000000000000006',
    'arbitrum': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'
} %}

WITH event_data AS (
    SELECT
        block_time,
        block_number,
        bytearray_substring(topic3, 13, 20) AS taker,
        CASE
            WHEN bytearray_substring(data, 13, 20) = 0x0000000000000000000000000000000000000000
            THEN {{ w_native[blockchain] }}
            ELSE bytearray_substring(data, 13, 20)
        END AS token_sold_address,
        CASE
            WHEN bytearray_substring(data, 45, 20) = 0x0000000000000000000000000000000000000000
            THEN {{ w_native[blockchain] }}
            ELSE bytearray_substring(data, 45, 20)
        END AS token_bought_address,
        bytearray_to_uint256(bytearray_substring(data, 65, 32)) AS token_sold_amount_raw,
        bytearray_to_uint256(bytearray_substring(data, 97, 32)) AS token_bought_amount_raw,
        contract_address AS project_contract_address,
        tx_hash,
        index AS evt_index,
        CAST(ARRAY[-1] AS ARRAY<BIGINT>) AS trace_address
    FROM {{ source(blockchain, 'logs') }}
    WHERE contract_address = {{ contract_address }}
        AND topic0 = 0xf37a3bd5c0f7f8527446af48a2a8fd5711deb58baa1a2747c3cb8dce6e9bb483
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% else %}
        AND block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain,
    'swapwizard' AS project,
    '1' AS version,
    TRY_CAST(date_trunc('DAY', e.block_time) AS date) AS block_date,
    TRY_CAST(date_trunc('MONTH', e.block_time) AS date) AS block_month,
    e.block_time,
    e.block_number,
    erc20_bought.symbol AS token_bought_symbol,
    erc20_sold.symbol AS token_sold_symbol,
    CASE
        WHEN LOWER(erc20_bought.symbol) > LOWER(erc20_sold.symbol)
        THEN CONCAT(erc20_sold.symbol, '-', erc20_bought.symbol)
        ELSE CONCAT(erc20_bought.symbol, '-', erc20_sold.symbol)
    END AS token_pair,
    e.token_bought_amount_raw / POWER(10, erc20_bought.decimals) AS token_bought_amount,
    e.token_sold_amount_raw / POWER(10, erc20_sold.decimals) AS token_sold_amount,
    e.token_bought_amount_raw,
    e.token_sold_amount_raw,
    COALESCE(
        (e.token_bought_amount_raw / POWER(10, erc20_bought.decimals)) * p_bought.price,
        (e.token_sold_amount_raw / POWER(10, erc20_sold.decimals)) * p_sold.price
    ) AS amount_usd,
    e.token_bought_address,
    e.token_sold_address,
    e.taker,
    CAST(NULL AS varbinary) AS maker,
    e.project_contract_address,
    e.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    e.trace_address,
    e.evt_index
FROM event_data e
INNER JOIN {{ source(blockchain, 'transactions') }} tx
    ON e.tx_hash = tx.hash
    {% if is_incremental() %}
    AND {{ incremental_predicate('tx.block_time') }}
    {% else %}
    AND tx.block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20_bought
    ON erc20_bought.contract_address = e.token_bought_address
    AND erc20_bought.blockchain = '{{ blockchain }}'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sold
    ON erc20_sold.contract_address = e.token_sold_address
    AND erc20_sold.blockchain = '{{ blockchain }}'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', e.block_time)
    AND p_bought.contract_address = e.token_bought_address
    AND p_bought.blockchain = '{{ blockchain }}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('p_bought.minute') }}
    {% else %}
    AND p_bought.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', e.block_time)
    AND p_sold.contract_address = e.token_sold_address
    AND p_sold.blockchain = '{{ blockchain }}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('p_sold.minute') }}
    {% else %}
    AND p_sold.minute >= TIMESTAMP '{{ project_start_date }}'
    {% endif %}

{% endmacro %}
