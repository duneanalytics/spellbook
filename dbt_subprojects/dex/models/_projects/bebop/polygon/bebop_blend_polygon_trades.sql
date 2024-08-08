{{ config(
    schema = 'bebop_blend_polygon',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
)}}

{% set project_start_date = '2024-04-23' %}

WITH
raw_call_data AS (
    SELECT
        fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order",
        ROW_NUMBER() OVER (PARTITION BY call_tx_hash) AS row_num
    FROM (
        SELECT
            'Single' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_settleSingle') }}
        UNION ALL
        SELECT
            'Single' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_settleSingleAndSignPermit') }}
        UNION ALL
        SELECT
            'Single' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_settleSingleAndSignPermit2') }}
        UNION ALL
        SELECT
            'Single' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_swapSingle') }}
        UNION ALL
        SELECT
            'Single' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_swapSingleFromContract') }}
        UNION ALL
        SELECT
            'Multi' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_settleMulti') }}
        UNION ALL
        SELECT
            'Multi' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_settleMultiAndSignPermit') }}
        UNION ALL
        SELECT
            'Multi' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_settleMultiAndSignPermit2') }}
        UNION ALL
        SELECT
            'Multi' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_swapMulti') }}
        UNION ALL
        SELECT
            'Aggregate' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_settleAggregate') }}
        UNION ALL
        SELECT
            'Aggregate' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_settleAggregateAndSignPermit') }}
        UNION ALL
        SELECT
            'Aggregate' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_settleAggregateAndSignPermit2') }}
        UNION ALL
        SELECT
            'Aggregate' as fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order"
        FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_call_swapAggregate') }}
    )
    WHERE call_success = True
    {% if is_incremental() %}
    AND {{ incremental_predicate('call_block_time') }}
    {% endif %}
),
raw_call_and_event_data AS (
    SELECT
         fun_type, call_block_time, call_block_number, call_tx_hash, contract_address, "order", evt_index
    FROM
        (SELECT
            evt_index, evt_tx_hash, evt_block_time, ROW_NUMBER() OVER (PARTITION BY evt_tx_hash ORDER BY evt_index) AS row_num
         FROM {{ source('bebop_pmms_polygon', 'BebopSettlement_evt_BebopOrder') }}
         {% if is_incremental() %}
         WHERE {{ incremental_predicate('evt_block_time') }}
         {% endif %}
         ) evt
         LEFT JOIN
         (SELECT
            fun_type, call_success, call_block_time, call_block_number, call_tx_hash, contract_address, "order", row_num
          FROM raw_call_data
         ) ex
         ON ex.call_tx_hash = evt.evt_tx_hash and ex.row_num = evt.row_num
),
bebop_single_trade AS (
    SELECT
        call_block_time AS block_time,
        call_block_number AS block_number,
        call_tx_hash AS tx_hash,
        evt_index,
        contract_address,
        from_hex(JSON_EXTRACT_SCALAR("order", '$.taker_address')) as taker_address,
        from_hex(JSON_EXTRACT_SCALAR("order", '$.maker_address')) as maker_address,
        from_hex(JSON_EXTRACT_SCALAR("order", '$.taker_token')) AS taker_token_address,
        from_hex(JSON_EXTRACT_SCALAR("order", '$.maker_token')) AS maker_token_address,
        JSON_EXTRACT_SCALAR("order", '$.taker_amount') AS taker_token_amount,
        JSON_EXTRACT_SCALAR("order", '$.maker_amount') AS maker_token_amount,
        'Simple-Swap' as trade_type,
        1 as taker_tokens_len,
        1 as maker_tokens_len,
        cast(array[0, 0, 0] as array<bigint>) as trace_address
    FROM
        raw_call_and_event_data
    WHERE fun_type = 'Single'
),
raw_bebop_multi_trade AS (
    SELECT
        call_block_time AS block_time,
        call_block_number AS block_number,
        call_tx_hash AS tx_hash,
        evt_index,
        contract_address,
        from_hex(JSON_EXTRACT_SCALAR("order", '$.taker_address')) as taker_address,
        from_hex(JSON_EXTRACT_SCALAR("order", '$.maker_address')) as maker_address,
        CAST(JSON_EXTRACT("order", '$.taker_tokens') AS ARRAY<VARCHAR>) AS taker_tokens,
        CAST(JSON_EXTRACT("order", '$.maker_tokens') AS ARRAY<VARCHAR>) AS maker_tokens,
        CAST(JSON_EXTRACT("order", '$.taker_amounts') AS ARRAY<VARCHAR>) AS taker_amounts,
        CAST(JSON_EXTRACT("order", '$.maker_amounts') AS ARRAY<VARCHAR>) AS maker_amounts,
        json_array_length(JSON_EXTRACT("order", '$.taker_amounts')) as taker_tokens_len,
        json_array_length(JSON_EXTRACT("order", '$.maker_amounts')) as maker_tokens_len,
        0 as order_index
    FROM
        raw_call_and_event_data
    WHERE fun_type = 'Multi'
),
raw_bebop_aggregate_trade AS (
    SELECT
        call_block_time AS block_time,
        call_block_number AS block_number,
        call_tx_hash AS tx_hash,
        evt_index,
        contract_address,
        from_hex(JSON_EXTRACT_SCALAR("order", '$.taker_address')) as taker_address,
        CAST(JSON_EXTRACT("order", '$.maker_addresses') AS ARRAY<VARCHAR>) AS maker_addresses,
        JSON_EXTRACT("order", '$.taker_tokens') AS taker_tokens_json,
        JSON_EXTRACT("order", '$.maker_tokens') AS maker_tokens_json,
        JSON_EXTRACT("order", '$.taker_amounts') AS taker_amounts_json,
        JSON_EXTRACT("order", '$.maker_amounts') AS maker_amounts_json,
        json_array_length(JSON_EXTRACT("order", '$.maker_addresses')) as orders_len
    FROM
        raw_call_and_event_data
    WHERE fun_type = 'Aggregate'
),
unnested_aggregate_orders AS (
    SELECT
        block_time,
        block_number,
        tx_hash,
        evt_index,
        contract_address,
        taker_address,
        from_hex(element_at(maker_addresses, sequence_number)) AS maker_address,
        CAST(json_extract(taker_tokens_json, concat('$[', cast(sequence_number - 1 as VARCHAR), ']')) AS ARRAY<VARCHAR>) AS taker_tokens,
        CAST(json_extract(maker_tokens_json, concat('$[', cast(sequence_number - 1 as VARCHAR), ']')) AS ARRAY<VARCHAR>) AS maker_tokens,
        CAST(json_extract(taker_amounts_json, concat('$[', cast(sequence_number - 1 as VARCHAR), ']')) AS ARRAY<VARCHAR>) AS taker_amounts,
        CAST(json_extract(maker_amounts_json, concat('$[', cast(sequence_number - 1 as VARCHAR), ']')) AS ARRAY<VARCHAR>) AS maker_amounts,
        json_array_length(json_extract(taker_tokens_json, concat('$[', cast(sequence_number - 1 as VARCHAR), ']'))) as taker_tokens_len,
        json_array_length(json_extract(maker_tokens_json, concat('$[', cast(sequence_number - 1 as VARCHAR), ']'))) as maker_tokens_len,
        sequence_number - 1 AS order_index
    FROM raw_bebop_aggregate_trade
    CROSS JOIN UNNEST(sequence(1, orders_len)) AS t(sequence_number)
),

unnested_taker_arrays AS (
    SELECT
        block_time,
        block_number,
        tx_hash,
        evt_index,
        contract_address,
        taker_address,
        maker_address,
        maker_tokens,
        maker_amounts,
        taker_tokens_len,
        maker_tokens_len,
        from_hex(element_at(taker_tokens, sequence_number)) AS taker_token_address,
        element_at(taker_amounts, sequence_number) AS taker_token_amount,
        order_index,
        sequence_number - 1 AS taker_token_index
    FROM (
        SELECT
            block_time, block_number, tx_hash, evt_index, contract_address, taker_address, maker_address,
            taker_tokens, maker_tokens, taker_amounts, maker_amounts, taker_tokens_len, maker_tokens_len, order_index
        FROM raw_bebop_multi_trade
        UNION
        SELECT
            block_time, block_number, tx_hash, evt_index, contract_address, taker_address, maker_address,
            taker_tokens, maker_tokens, taker_amounts, maker_amounts, taker_tokens_len, maker_tokens_len, order_index
        FROM unnested_aggregate_orders
    )
    CROSS JOIN UNNEST(sequence(1, taker_tokens_len)) AS t(sequence_number)
),
bebop_multi_and_aggregate_trades AS (
    SELECT
        block_time,
        block_number,
        tx_hash,
        evt_index,
        contract_address,
        taker_address,
        maker_address,
        taker_token_address,
        from_hex(element_at(maker_tokens, sequence_number)) AS maker_token_address,
        taker_token_amount,
        element_at(maker_amounts, sequence_number) AS maker_token_amount,
        CASE
          WHEN taker_tokens_len = 1 AND maker_tokens_len > 1 THEN 'Multi-Buy'
          WHEN maker_tokens_len = 1 AND taker_tokens_len > 1 THEN 'Multi-Sell'
          ELSE 'Simple-Swap'
        END as trade_type,
        taker_tokens_len,
        maker_tokens_len,
        cast(array[order_index, taker_token_index, sequence_number - 1] as array<bigint>) as trace_address
    FROM unnested_taker_arrays
    CROSS JOIN UNNEST(sequence(1, maker_tokens_len)) AS t(sequence_number)
),
all_trades as (
  SELECT
    block_time, block_number, tx_hash, evt_index, contract_address, taker_address, maker_address, taker_token_address,
    maker_token_address, taker_token_amount, maker_token_amount, trade_type, taker_tokens_len, maker_tokens_len, trace_address,
    ROW_NUMBER() OVER (PARTITION BY tx_hash, evt_index, trace_address ORDER BY evt_index) AS row_num
  FROM (
    SELECT
        block_time, block_number, tx_hash, evt_index, contract_address, taker_address, maker_address, taker_token_address,
        maker_token_address, taker_token_amount, maker_token_amount, trade_type, taker_tokens_len, maker_tokens_len, trace_address
    FROM bebop_single_trade
    UNION ALL
    SELECT
        block_time, block_number, tx_hash, evt_index, contract_address, taker_address, maker_address, taker_token_address,
        maker_token_address, taker_token_amount, maker_token_amount, trade_type, taker_tokens_len, maker_tokens_len, trace_address
    FROM bebop_multi_and_aggregate_trades
    )
)

SELECT
  'polygon' AS blockchain,
  'bebop' AS project,
  'blend' AS version,
  CAST(date_trunc('DAY', t.block_time) AS date) AS block_date,
  CAST(date_trunc('MONTH', t.block_time) AS date) AS block_month,
  t.block_time AS block_time,
  t.trade_type,
  t_bought.symbol AS token_bought_symbol,
  t_sold.symbol AS token_sold_symbol,
  CASE
    WHEN lower(t_bought.symbol) > lower(t_sold.symbol) THEN concat(t_sold.symbol, '-', t_bought.symbol)
    ELSE concat(t_bought.symbol, '-', t_sold.symbol)
  END AS token_pair,
  CAST(t.maker_token_amount as double) / power(10, coalesce(t_bought.decimals, 0)) AS token_bought_amount,
  CAST(t.taker_token_amount as double) / power(10, coalesce(t_sold.decimals, 0)) AS token_sold_amount,
  CAST(t.maker_token_amount as UINT256) AS token_bought_amount_raw,
  CAST(t.taker_token_amount as UINT256) as token_sold_amount_raw,
  CASE 
    WHEN t.trade_type = 'Multi-Buy' THEN COALESCE(
        (CAST(t.maker_token_amount as double) / power(10, t_bought.decimals)) * p_bought.price,
        (CAST(t.taker_token_amount as double) / power(10, t_sold.decimals)) * p_sold.price / maker_tokens_len
    )
    WHEN t.trade_type = 'Multi-Sell' THEN COALESCE(
        (CAST(t.taker_token_amount as double) / power(10, t_sold.decimals)) * p_sold.price,
        (CAST(t.maker_token_amount as double) / power(10, t_bought.decimals)) * p_bought.price / taker_tokens_len
    )
    ELSE COALESCE(
        (CAST(t.maker_token_amount as double) / power(10, t_bought.decimals)) * p_bought.price,
        (CAST(t.taker_token_amount as double) / power(10, t_sold.decimals)) * p_sold.price
    ) 
  END as amount_usd,
  t.maker_token_address AS token_bought_address,
  t.taker_token_address AS token_sold_address,
  t.taker_address AS taker,
  t.maker_address AS maker,
  t.contract_address AS project_contract_address,
  t.tx_hash,
  tx."from" AS tx_from,
  tx.to AS tx_to,
  t.trace_address,
  t.evt_index
  FROM (select * from all_trades where row_num = 1) t
  INNER JOIN
  {{ source('polygon', 'transactions')}} tx
    ON t.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND {{ incremental_predicate('tx.block_time') }}
    {% endif %}
  LEFT JOIN 
  {{ source('tokens', 'erc20') }} t_bought 
    ON t_bought.contract_address = t.maker_token_address
    AND t_bought.blockchain = 'polygon'
  LEFT JOIN 
  {{ source('tokens', 'erc20') }} t_sold
    ON t_sold.contract_address = t.taker_token_address
    AND t_sold.blockchain = 'polygon'
  LEFT JOIN 
  {{ source('prices', 'usd') }} p_bought 
    ON p_bought.minute = date_trunc('minute', t.block_time)
    AND p_bought.contract_address = t.maker_token_address
    AND p_bought.blockchain = 'polygon'
  {% if not is_incremental() %}
  AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
  {% endif %}
  {% if is_incremental() %}
  AND {{ incremental_predicate('p_bought.minute') }}
  {% endif %}
  LEFT JOIN 
  {{ source('prices', 'usd') }} p_sold 
    ON p_sold.minute = date_trunc('minute', t.block_time)
    AND p_sold.contract_address = t.taker_token_address
    AND p_sold.blockchain = 'polygon'
  {% if not is_incremental() %}
  AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
  {% endif %}
  {% if is_incremental() %}
  AND {{ incremental_predicate('p_sold.minute') }}
  {% endif %}