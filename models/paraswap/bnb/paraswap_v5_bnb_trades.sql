{{ config(
    schema = 'paraswap_v5_bnb',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "paraswap_v5",
                                \'["springzh"]\') }}'
    )
}}

{% set project_start_date = '2021-08-19' %}
{% set trade_event_tables = [
    source('paraswap_bnb', 'AugustusSwapperV5_evt_Bought')
    ,source('paraswap_bnb', 'AugustusSwapperV5_evt_Bought2')
    ,source('paraswap_bnb', 'AugustusSwapperV5_evt_BoughtV3')
    ,source('paraswap_bnb', 'AugustusSwapperV5_evt_Swapped')
    ,source('paraswap_bnb', 'AugustusSwapperV5_evt_Swapped2')
    ,source('paraswap_bnb', 'AugustusSwapperV5_evt_SwappedV3')
] %}

WITH dexs AS (
    {% for trade_table in trade_event_tables %}
        SELECT 
            evt_block_time AS block_time,
            evt_block_number AS block_number,
            beneficiary AS taker, 
            initiator AS maker, 
            receivedAmount AS token_bought_amount_raw,
            srcAmount AS token_sold_amount_raw,
            CAST(NULL AS double) AS amount_usd,
            CASE 
                WHEN destToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' -- WBNB 
                ELSE destToken
            END AS token_bought_address,
            CASE 
                WHEN srcToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
                THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' -- WBNB 
                ELSE srcToken
            END AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash, 
            CAST(ARRAY() AS array<bigint>) AS trace_address,
            evt_index
        FROM {{ trade_table }} p 
        {% if is_incremental() %}
        WHERE p.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT 'bnb' AS blockchain,
    'paraswap' AS project,
    '5' AS version,
    try_cast(date_trunc('DAY', d.block_time) AS date) AS block_date,
    d.block_time,
    e1.symbol AS token_bought_symbol,
    e2.symbol AS token_sold_symbol,
    CASE
        WHEN lower(e1.symbol) > lower(e2.symbol) THEN concat(e2.symbol, '-', e1.symbol)
        ELSE concat(e1.symbol, '-', e2.symbol)
    END AS token_pair,
    d.token_bought_amount_raw / power(10, e1.decimals) AS token_bought_amount,
    d.token_sold_amount_raw / power(10, e2.decimals) AS token_sold_amount,
    CAST(d.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw,
    CAST(d.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw,
    coalesce(
        d.amount_usd
        ,(d.token_bought_amount_raw / power(10, p1.decimals)) * p1.price
        ,(d.token_sold_amount_raw / power(10, p2.decimals)) * p2.price
    ) AS amount_usd,
    d.token_bought_address,
    d.token_sold_address,
    coalesce(d.taker, tx.from) AS taker,
    d.maker,
    d.project_contract_address,
    d.tx_hash,
    tx.from AS tx_from,
    tx.to AS tx_to,
    d.trace_address,
    d.evt_index
FROM dexs d
INNER JOIN {{ source('bnb', 'transactions') }} tx ON d.tx_hash = tx.hash
    AND d.block_number = tx.block_number
    {% if not is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} e1 ON e1.contract_address = d.token_bought_address
    AND e1.blockchain = 'bnb'
LEFT JOIN {{ ref('tokens_erc20') }} e2 on e2.contract_address = d.token_sold_address
    AND e2.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} p1 ON p1.minute = date_trunc('minute', d.block_time)
    AND p1.contract_address = d.token_bought_address
    AND p1.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p1.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p1.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p2 ON p2.minute = date_trunc('minute', d.block_time)
    AND p2.contract_address = d.token_sold_address
    AND p2.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p2.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p2.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
