{{ config(
	tags=['legacy'],
	
    alias = alias('trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
     post_hook='{{ expose_spells(\'["ethereum"]\',
                                      "project",
                                      "bebop",
                                    \'["alekss"]\') }}'
    )
}}

{% set project_start_date = '2023-03-30' %}

WITH
  bebop_raw_data AS (
    SELECT
      call_block_time AS block_time,
      call_block_number AS block_number,
      call_tx_hash AS tx_hash,
      evt_index,
      ex.contract_address,
      get_json_object(order, '$.expiry') AS expiry,
      cast(
        get_json_object(order, '$.taker_address') AS string
      ) AS taker_address,
      array_distinct(
        from_json(
          get_json_object(order, '$.maker_addresses'), 'array<string>'
        )
      ) AS maker_addresses,
      arrays_zip(
        array_distinct(
          flatten(
            from_json(
              get_json_object(order, '$.taker_tokens') , 'array<array<string>>'
            )
          )
        ),
        array_distinct(
          flatten(
            from_json(
              get_json_object(order, '$.taker_amounts') , 'array<array<DECIMAL(38, 0)>>'
            )
          )
        )
      ) AS taker_tokens,
      arrays_zip(
        array_distinct(
          flatten(
            from_json(
              get_json_object(order, '$.maker_tokens') , 'array<array<string>>'
            )
          )
        ),
        array_distinct(
          flatten(
            from_json(
              get_json_object(order, '$.maker_amounts') , 'array<array<DECIMAL(38, 0)>>'
            )
          )
        )
      ) AS maker_tokens
    FROM
      {{ source('bebop_v3_ethereum', 'BebopAggregationContract_evt_AggregateOrderExecuted') }}
      LEFT JOIN {{ source('bebop_v3_ethereum', 'BebopAggregationContract_call_SettleAggregateOrder') }} ex ON ex.call_tx_hash = evt_tx_hash
    WHERE
      call_success = true
      {% if not is_incremental() %}
      AND evt_block_time >= '{{project_start_date}}'
      {% endif %}
      {% if is_incremental() %}
      AND evt_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
  ),
  explode_taker_tokens_data as (
    SELECT
      *,
      posexplode (taker_tokens) as (taker_ind, taker_token_pair)
    FROM
      bebop_raw_data
  ),
  explode_tokens_data as (
    SELECT
      *,
      posexplode (maker_tokens) as (maker_ind, maker_token_pair)
    FROM
      explode_taker_tokens_data
  ),
  simple_trades as (
    SELECT
      block_time,
      block_number,
      contract_address,
      tx_hash,
      evt_index,
      taker_address,
      cast(array(taker_ind, maker_ind) as array<bigint>) as trace_address,
      taker_token_pair.`0` as taker_token,
      (taker_token_pair.`1` / cardinality(maker_tokens)) as taker_amount,
      maker_token_pair.`0` as maker_token,
      (maker_token_pair.`1` / cardinality(taker_tokens)) as maker_amount
    FROM
      explode_tokens_data
  )
SELECT
  'ethereum' AS blockchain,
  'bebop' AS project,
  '1' AS version,
  TRY_CAST(date_trunc('DAY', t.block_time) AS date) AS block_date,
  t.block_time AS block_time,
  t_bought.symbol AS token_bought_symbol,
  t_sold.symbol AS token_sold_symbol,
  CASE
    WHEN lower(t_bought.symbol) > lower(t_sold.symbol) THEN concat(t_sold.symbol, '-', t_bought.symbol)
    ELSE concat(t_bought.symbol, '-', t_sold.symbol)
  END AS token_pair,
  cast(t.maker_amount AS DECIMAL (38, 0)) / power(10, coalesce(t_bought.decimals, 0)) AS token_bought_amount,
  cast(t.taker_amount AS DECIMAL (38, 0)) / power(10, coalesce(t_sold.decimals, 0)) AS token_sold_amount,
  cast(t.maker_amount AS DECIMAL (38, 0)) AS token_bought_amount_raw,
  cast(t.taker_amount AS DECIMAL (38, 0)) AS token_sold_amount_raw,
  coalesce(
    (maker_amount / power(10, coalesce(p_bought.decimals, 0))) * p_bought.price,
    (taker_amount / power(10, coalesce(p_sold.decimals, 0))) * p_sold.price
  ) AS amount_usd,
  t_bought.contract_address AS token_bought_address,
  t_sold.contract_address AS token_sold_address,
  t.taker_address AS taker,
  t.contract_address AS maker,
  t.contract_address AS project_contract_address,
  t.tx_hash,
  t.taker_address AS tx_from,
  t.contract_address AS tx_to,
  t.trace_address,
  t.evt_index
FROM
  simple_trades t
  LEFT JOIN {{ ref('tokens_erc20_legacy') }} t_bought ON cast(t_bought.contract_address AS string) = t.maker_token
  AND t_bought.blockchain = 'ethereum'
  LEFT JOIN {{ ref('tokens_erc20_legacy') }} t_sold ON cast(t_sold.contract_address AS string) = t.taker_token
  AND t_sold.blockchain = 'ethereum'
  LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', t.block_time)
  AND cast(p_bought.contract_address AS string) = t.maker_token
  AND p_bought.blockchain = 'ethereum'
  {% if not is_incremental() %}
  AND p_bought.minute >= '{{project_start_date}}'
  {% endif %}
  {% if is_incremental() %}
  AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
  {% endif %}
  LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.minute = date_trunc('minute', t.block_time)
  AND cast(p_sold.contract_address AS string) = t.taker_token
  AND p_sold.blockchain = 'ethereum'
  {% if not is_incremental() %}
  AND p_sold.minute >= '{{project_start_date}}'
  {% endif %}
  {% if is_incremental() %}
  AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
  {% endif %}