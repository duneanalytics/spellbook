{% macro 
    chainlink_ocr_gas_transmission_logs(
        blockchain
    ) 
%}

SELECT
  '{{blockchain}}' as blockchain,
  block_hash,
  contract_address,
  data,
  topic0,
  topic1,
  topic2,
  topic3,
  tx_hash,
  block_number,
  block_time,
  index,
  tx_index
FROM
  {{ source(blockchain, 'logs') }} logs
WHERE
  topic0 = 0xf6a97944f31ea060dfde0566e4167c1a1082551e64b60ecb14d599a9d023d451


{% endmacro %}


----------------------------------------------

{% macro 
    chainlink_ocr_reward_transmission_logs(
        blockchain
    ) 
%}

SELECT
  '{{blockchain}}' as blockchain,
  block_hash,
  contract_address,
  data,
  topic0,
  topic1,
  topic2,
  topic3,
  tx_hash,
  block_number,
  block_time,
  index,
  tx_index
FROM
  {{ source(blockchain, 'logs') }} logs
WHERE
  topic0 = 0xd0d9486a2c673e2a4b57fc82e4c8a556b3e2b82dd5db07e2c04a920ca0f469b6

{% endmacro %}

{% macro 
    chainlink_ocr_fulfilled_transactions(
        blockchain,
        gas_token_symbol,
        gas_price_column
    ) 
%}

WITH
  network_usd AS (
    SELECT
      minute as block_time,
      price as usd_amount
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      symbol = '{{gas_token_symbol}}'
      {% if is_incremental() %}
        AND {{ incremental_predicate('minute') }}
      {% endif %}      
  ),
  ocr_fulfilled_transactions AS (
    SELECT
      tx.hash as tx_hash,
      tx.index as tx_index,
      MAX(tx.block_time) as block_time,
      cast(date_trunc('month', MAX(tx.block_time)) as date) as date_month,
      tx."from" as "node_address",
      MAX(
        (cast((gas_used) as double) / 1e18) * tx.{{gas_price_column}}
      ) as token_amount,
      MAX(network_usd.usd_amount) as usd_amount
    FROM
      {{ source(blockchain, 'transactions') }} tx
      RIGHT JOIN {{ ref('chainlink_' ~ blockchain ~ '_ocr_gas_transmission_logs') }} ocr_gas_transmission_logs ON ocr_gas_transmission_logs.tx_hash = tx.hash
      LEFT JOIN network_usd ON date_trunc('minute', tx.block_time) = network_usd.block_time
    {% if is_incremental() %}
      WHERE {{ incremental_predicate('tx.block_time') }}
    {% endif %}      
    GROUP BY
      tx.hash,
      tx.index,
      tx."from"
  )
SELECT
 '{{blockchain}}' as blockchain,
  block_time,
  date_month,
  node_address,
  token_amount,
  usd_amount,
  tx_hash,
  tx_index
FROM
  ocr_fulfilled_transactions

{% endmacro %}

-----------------------------------------------

{% macro 
    chainlink_ocr_reverted_transactions(
        blockchain,
        gas_token_symbol
    ) 
%}

WITH
  network_usd AS (
    SELECT
      minute as block_time,
      price as usd_amount
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      symbol = '{{gas_token_symbol}}'
      {% if is_incremental() %}
        AND {{ incremental_predicate('minute') }}
      {% endif %}      
  ),
  ocr_reverted_transactions AS (
    SELECT
      tx.hash as tx_hash,
      tx.index as tx_index,
      MAX(tx.block_time) as block_time,
      cast(date_trunc('month', MAX(tx.block_time)) as date) as date_month,
      tx."from" as "node_address",
      MAX((cast((gas_used) as double) / 1e18) * gas_price) as token_amount,
      MAX(network_usd.usd_amount) as usd_amount
    FROM
      {{ source(blockchain, 'transactions') }} tx
      LEFT JOIN network_usd ON date_trunc('minute', tx.block_time) = network_usd.block_time
    WHERE
      success = false
      {% if is_incremental() %}
        AND {{ incremental_predicate('tx.block_time') }}
      {% endif %}      
    GROUP BY
      tx.hash,
      tx.index,
      tx."from"
  )
SELECT
 '{{blockchain}}' as blockchain,
  block_time,
  date_month,
  node_address,
  token_amount,
  usd_amount,
  tx_hash,
  tx_index
FROM
  ocr_reverted_transactions

{% endmacro %}

----------------------------------------------

{% macro 
    chainlink_ocr_request_daily(
        blockchain
    ) 
%}

{% set truncate_by = 'day' %}

WITH
  ocr_request_daily_meta AS (
    SELECT
      COALESCE(
        cast(date_trunc('{{truncate_by}}', fulfilled.block_time) as date),
        cast(date_trunc('{{truncate_by}}', reverted.block_time) as date)
      ) AS "date_start",      
      COALESCE(
        fulfilled.node_address,
        reverted.node_address
      ) AS "node_address",
      COALESCE(COUNT(fulfilled.token_amount), 0) as fulfilled_requests,
      COALESCE(COUNT(reverted.token_amount), 0) as reverted_requests,
      COALESCE(COUNT(fulfilled.token_amount), 0) + COALESCE(COUNT(reverted.token_amount), 0) as total_requests
    FROM
      {{ ref('chainlink_'~ blockchain ~ '_ocr_fulfilled_transactions') }} fulfilled
      FULL OUTER JOIN {{ ref('chainlink_' ~ blockchain ~ '_ocr_reverted_transactions') }} reverted ON
        reverted.block_time = fulfilled.block_time AND
        reverted.node_address = fulfilled.node_address
    {% if is_incremental() %}
      WHERE
        {{ incremental_predicate('fulfilled.block_time') }}
        OR {{ incremental_predicate('reverted.block_time') }}
    {% endif %}
    GROUP BY
      1, 2
    ORDER BY
      1, 2
  ),
  ocr_request_daily AS (
    SELECT
      '{{blockchain}}' as blockchain,
      date_start,
      cast(date_trunc('month', date_start) as date) as date_month,
      ocr_request_daily_meta.node_address as node_address,
      operator_name,
      fulfilled_requests,
      reverted_requests,
      total_requests
    FROM ocr_request_daily_meta
    LEFT JOIN {{ ref('chainlink_' ~ blockchain ~ '_ocr_operator_node_meta') }} ocr_operator_node_meta ON ocr_operator_node_meta.node_address = ocr_request_daily_meta.node_address
  )
SELECT 
  blockchain,
  date_start,
  date_month,
  node_address,
  operator_name,
  fulfilled_requests,
  reverted_requests,
  total_requests
FROM
  ocr_request_daily
ORDER BY
  "date_start"

{% endmacro %}

---------------------------------------------

{% macro 
    chainlink_ocr_reward_daily(
        blockchain
    ) 
%}

WITH
  admin_address_meta as (
    SELECT DISTINCT
      admin_address
    FROM
      {{ref('chainlink_' ~ blockchain ~ '_ocr_reward_evt_transfer_daily')}} ocr_reward_evt_transfer_daily
  ),
  link_usd_daily AS (
    SELECT
      cast(date_trunc('day', price.minute) as date) as "date_start",
      MAX(price.price) as usd_amount
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      price.symbol = 'LINK'
      {% if is_incremental() %}
        AND {{ incremental_predicate('price.minute') }}
      {% endif %}      
    GROUP BY
      1
    ORDER BY
      1
  ),
  link_usd_daily_expanded_by_admin_address AS (
    SELECT
      date_start,
      usd_amount,
      admin_address
    FROM
      link_usd_daily
    CROSS JOIN
      admin_address_meta
    ORDER BY
      date_start,
      admin_address
  ),
  payment_meta AS (
    SELECT
      date_start,
      link_usd_daily_expanded_by_admin_address.admin_address as admin_address,
      usd_amount,
      (
        SELECT
          MAX(ocr_reward_evt_transfer_daily.date_start)
        FROM
          {{ref('chainlink_' ~ blockchain ~ '_ocr_reward_evt_transfer_daily')}} ocr_reward_evt_transfer_daily
        WHERE
          ocr_reward_evt_transfer_daily.date_start <= link_usd_daily_expanded_by_admin_address.date_start
          AND ocr_reward_evt_transfer_daily.admin_address = link_usd_daily_expanded_by_admin_address.admin_address
      ) as prev_payment_date,
      (
        SELECT
          MIN(ocr_reward_evt_transfer_daily.date_start)
        FROM
          {{ref('chainlink_' ~ blockchain ~ '_ocr_reward_evt_transfer_daily')}} ocr_reward_evt_transfer_daily
        WHERE
          ocr_reward_evt_transfer_daily.date_start > link_usd_daily_expanded_by_admin_address.date_start
          AND ocr_reward_evt_transfer_daily.admin_address = link_usd_daily_expanded_by_admin_address.admin_address
      ) as next_payment_date
    FROM
      link_usd_daily_expanded_by_admin_address
    ORDER BY
      1, 2
  ),
  ocr_reward_daily AS (
    SELECT 
      payment_meta.date_start,
      cast(date_trunc('month', payment_meta.date_start) as date) as date_month,
      payment_meta.admin_address,
      ocr_operator_admin_meta.operator_name,      
      COALESCE(ocr_reward_evt_transfer_daily.token_amount / EXTRACT(DAY FROM next_payment_date - prev_payment_date), 0) as token_amount,
      (COALESCE(ocr_reward_evt_transfer_daily.token_amount / EXTRACT(DAY FROM next_payment_date - prev_payment_date), 0) * payment_meta.usd_amount) as usd_amount
    FROM 
      payment_meta
    LEFT JOIN 
      {{ref('chainlink_' ~ blockchain ~ '_ocr_reward_evt_transfer_daily')}} ocr_reward_evt_transfer_daily ON
        payment_meta.next_payment_date = ocr_reward_evt_transfer_daily.date_start AND
        payment_meta.admin_address = ocr_reward_evt_transfer_daily.admin_address
    LEFT JOIN {{ ref('chainlink_' ~ blockchain ~ '_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = ocr_reward_evt_transfer_daily.admin_address
    ORDER BY date_start
  )
SELECT
  '{{blockchain}}' as blockchain,
  date_start,
  date_month,
  admin_address,
  operator_name,
  token_amount,
  usd_amount
FROM 
  ocr_reward_daily
ORDER BY
  2, 4


{% endmacro %}

---------------------------------------------

{% macro 
    chainlink_ocr_reward_evt_transfer_daily(
        blockchain
    ) 
%}

SELECT
  '{{blockchain}}' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  ocr_reward_evt_transfer.admin_address as admin_address,
  MAX(ocr_reward_evt_transfer.operator_name) as operator_name,
  SUM(token_value) as token_amount
FROM
  {{ref('chainlink_' ~ blockchain ~ '_ocr_reward_evt_transfer')}} ocr_reward_evt_transfer
  LEFT JOIN {{ ref('chainlink_' ~ blockchain ~ '_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = ocr_reward_evt_transfer.admin_address
{% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}      
GROUP BY
  2, 4
ORDER BY
  2, 4

{% endmacro %}

---------------------------------------------

{% macro 
    chainlink_ocr_reward_evt_transfer(
        blockchain
    ) 
%}

SELECT
  '{{blockchain}}' as blockchain,
  to as admin_address,
  MAX(operator_name) as operator_name,
  MAX(reward_evt_transfer.evt_block_time) as evt_block_time,
  MAX(cast(reward_evt_transfer.value as double) / 1e18) as token_value
FROM
  {{ source('erc20_' ~ blockchain, 'evt_transfer') }} reward_evt_transfer
  RIGHT JOIN {{ ref('chainlink_' ~ blockchain ~ '_ocr_reward_transmission_logs') }} ocr_reward_transmission_logs ON ocr_reward_transmission_logs.contract_address = reward_evt_transfer."from"
  LEFT JOIN {{ ref('chainlink_' ~ blockchain ~ '_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = reward_evt_transfer.to
WHERE
  reward_evt_transfer."from" IN (ocr_reward_transmission_logs.contract_address)
GROUP BY
  evt_tx_hash,
  evt_index,
  to

{% endmacro %}


---------------------------------------------

{% macro 
    chainlink_ocr_gas_daily(
        blockchain
    ) 
%}

{% set truncate_by = 'day' %}

WITH
  ocr_gas_fulfilled_daily AS (
    SELECT
      cast(date_trunc('{{truncate_by}}', fulfilled.block_time) as date) as date_start,
      fulfilled.node_address,
      SUM(fulfilled.token_amount) as token_amount,
      SUM(fulfilled.token_amount * fulfilled.usd_amount) as usd_amount
    FROM
      {{ ref('chainlink_' ~ blockchain ~ '_ocr_fulfilled_transactions') }} fulfilled
    {% if is_incremental() %}
      WHERE
        {{ incremental_predicate('fulfilled.block_time') }}
    {% endif %}
    GROUP BY
      1, 2
    ORDER BY
      1, 2
  ),
  ocr_gas_reverted_daily AS (
    SELECT
      cast(date_trunc('{{truncate_by}}', reverted.block_time) as date) as date_start,
      reverted.node_address,
      SUM(reverted.token_amount) as token_amount,
      SUM(reverted.token_amount * reverted.usd_amount) as usd_amount
    FROM
      {{ ref('chainlink_' ~ blockchain ~ '_ocr_reverted_transactions') }} reverted
    {% if is_incremental() %}
      WHERE
         {{ incremental_predicate('reverted.block_time') }}
    {% endif %}
    GROUP BY
      1, 2
    ORDER BY
      1, 2
  ),
  ocr_gas_daily_meta AS (
    SELECT
      COALESCE(
        fulfilled.date_start,
        reverted.date_start
      ) AS "date_start",      
      COALESCE(
        fulfilled.node_address,
        reverted.node_address
      ) AS "node_address",
      COALESCE(fulfilled.token_amount, 0) as fulfilled_token_amount,
      COALESCE(reverted.token_amount, 0) as reverted_token_amount,
      COALESCE(fulfilled.usd_amount, 0) as fulfilled_usd_amount,
      COALESCE(reverted.usd_amount, 0) as reverted_usd_amount
    FROM
      ocr_gas_fulfilled_daily fulfilled
      FULL OUTER JOIN ocr_gas_reverted_daily reverted ON
        reverted.date_start = fulfilled.date_start AND
        reverted.node_address = fulfilled.node_address
    ORDER BY
      1, 2
  ),
  ocr_gas_daily AS (
    SELECT
      '{{blockchain}}' as blockchain,
      date_start,
      cast(date_trunc('month', date_start) as date) as date_month,
      ocr_gas_daily_meta.node_address as node_address,
      operator_name,
      fulfilled_token_amount,
      fulfilled_usd_amount,
      reverted_token_amount,
      reverted_usd_amount,
      fulfilled_token_amount + reverted_token_amount as total_token_amount,
      fulfilled_usd_amount + reverted_usd_amount as total_usd_amount
    FROM ocr_gas_daily_meta
    LEFT JOIN {{ ref('chainlink_' ~ blockchain ~ '_ocr_operator_node_meta') }} ocr_operator_node_meta ON ocr_operator_node_meta.node_address = ocr_gas_daily_meta.node_address
  )
SELECT 
  blockchain,
  date_start,
  date_month,
  node_address,
  operator_name,
  fulfilled_token_amount,
  fulfilled_usd_amount,
  reverted_token_amount,
  reverted_usd_amount,
  total_token_amount,
  total_usd_amount    
FROM
  ocr_gas_daily
ORDER BY
  "date_start"

{% endmacro %}