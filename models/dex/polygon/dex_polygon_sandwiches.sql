{{ config(
	tags=['legacy'],
	
        schema = 'dex_polygon',
        alias = alias('sandwiches', legacy_model=True),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['sandwiched_pool', 'frontrun_tx_hash', 'frontrun_taker', 'frontrun_index', 'currency_address'],
        post_hook='{{ expose_spells(\'["polygon"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}

WITH trades AS (
    SELECT dt.project
    , dt.version
    , date_trunc("day", dt.block_time) AS block_date
    , dt.block_time
    , t.block_number
    , dt.token_sold_address
    , dt.token_bought_address
    , dt.token_sold_symbol
    , dt.token_bought_symbol
    , dt.taker
    , dt.tx_hash
    , dt.tx_from
    , dt.project_contract_address
    , dt.evt_index
    , t.index
    , MIN(t.gas_price) AS gas_price
    , MIN((t.gas_price/POWER(10, 18))*t.gas_used) AS tx_fee
    , SUM(COALESCE(dt.token_sold_amount_raw, 0)) AS token_sold_amount_raw
    , SUM(COALESCE(dt.token_bought_amount_raw, 0)) AS token_bought_amount_raw
    , SUM(COALESCE(dt.token_sold_amount, 0)) AS token_sold_amount
    , SUM(COALESCE(dt.token_bought_amount, 0)) AS token_bought_amount
    , SUM(COALESCE(dt.amount_usd, 0)) AS amount_usd
    FROM {{ ref('dex_trades_legacy') }} dt
    INNER JOIN {{ source('polygon', 'transactions') }} t ON t.block_time=dt.block_time
        AND t.hash=dt.tx_hash
        {% if is_incremental() %}
        AND t.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE dt.blockchain = 'polygon'
    {% if is_incremental() %}
    AND block_date >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    )

, sandwiches AS (
    SELECT 'polygon' AS blockchain
    , s1.project
    , s1.version
    , s1.block_date
    , s1.block_time
    , s1.block_number
    , SUM(s2.token_sold_amount_raw)/SUM(s1.token_bought_amount_raw) AS ratio_traded_token
    , SUM(s2.token_bought_amount_raw)/SUM(s1.token_sold_amount_raw) AS profit_percentage_of_initial
    , SUM(s2.token_bought_amount_raw)-SUM(s1.token_sold_amount_raw) AS profit_amount_raw
    , SUM(s2.token_bought_amount)-SUM(s1.token_sold_amount) AS profit_amount
    , s1.token_sold_address AS currency_address
    , s1.token_sold_symbol AS currency_symbol
    , SUM(s1.token_bought_amount_raw)-SUM(s2.token_sold_amount_raw) AS profit_traded_currency_amount_raw
    , SUM(s1.token_bought_amount)-SUM(s2.token_sold_amount) AS profit_traded_currency_amount
    , s1.token_bought_address AS traded_currency_address
    , s1.token_bought_symbol AS traded_currency_symbol
    , s1.taker AS frontrun_taker
    , s1.tx_from AS frontrun_tx_from
    , MAX_BY(s2.taker, s2.index) AS backrun_taker
    , MAX_BY(s2.tx_from, s2.index) AS backrun_tx_from
    , s1.index AS frontrun_index
    , MAX_BY(s2.index, s2.index) AS backrun_index
    , s1.project_contract_address AS sandwiched_pool
    , s1.tx_hash AS frontrun_tx_hash
    , MAX_BY(s2.tx_hash, s2.index) AS backrun_tx_hash
    , MAX_BY(s2.index, s2.index)-s1.index-1 AS amount_trades_between
    , s1.gas_price
    , s1.tx_fee AS frontrun_tx_fee
    , MAX_BY(s2.tx_fee, s2.index) AS backrun_tx_fee
    FROM trades s1
    INNER JOIN trades s2 ON s1.block_time=s2.block_time
        AND s1.project=s2.project
        AND s1.version=s2.version
        AND s1.tx_hash!=s2.tx_hash
        AND s1.index<s2.index
        AND s1.project_contract_address=s2.project_contract_address
        AND (s1.tx_from=s2.tx_from OR s1.taker=s2.taker)
        AND s1.token_sold_address=s2.token_bought_address
        AND s1.token_bought_address=s2.token_sold_address
        AND s2.token_sold_amount BETWEEN s1.token_bought_amount*0.9 AND s1.token_bought_amount*1.1
        --AND s2.token_bought_amount > s1.token_sold_amount -- Removed to also include trades where the sandwiched trade was unprofitable
    GROUP BY s1.project, s1.version, s1.block_date, s1.block_time, s1.block_number, s1.token_sold_address, s1.token_sold_symbol
    , s1.token_bought_address, s1.token_bought_symbol, s1.taker, s1.tx_from, s1.index, s1.project_contract_address, s1.tx_hash
    , s1.gas_price, s1.tx_fee
    )

SELECT s1.*
FROM sandwiches s1
LEFT JOIN sandwiches s2 ON s1.block_time=s2.block_time
    AND s1.frontrun_tx_hash=s2.backrun_tx_hash
WHERE s2.backrun_index IS NULL
