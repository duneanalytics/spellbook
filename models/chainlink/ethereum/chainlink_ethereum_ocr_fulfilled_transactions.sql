{{ config(
    alias = 'ocr_fulfilled_transactions',
    partition_by = ['hash', 'index', 'from'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['hash', 'index', 'from'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
    )
}}

{% set incremental_interval = '1 week' %}

WITH
  ethereum_usd AS (
    SELECT
      minute as block_time,
      price as usd_value
    FROM
      prices.usd
    WHERE
      symbol = 'ETH'
      {% if is_incremental() %}
        AND minute >= date_trunc("day", now() - interval '{{incremental_interval}}')
      {% endif %}      
  ),
  ocr_fulfilled_transactions AS (
    SELECT
      MAX(tx.block_time) as block_time,
      tx."from" as "node_address",
      MAX((cast((gas_used) as double) / 1e18) * gas_price) as token_amount,
      MAX(ethereum_usd.usd_value) as usd_value
    FROM
      ethereum.transactions as tx
      RIGHT JOIN {{ ref('chainlink_ethereum_ocr_transmission_logs') }} ON ocr_transmission_logs.tx_hash = tx.hash
      LEFT JOIN ethereum_usd ON date_trunc('minute', tx.block_time) = ethereum_usd.block_time
    {% if is_incremental() %}
      WHERE tx.block_time >= date_trunc("day", now() - interval '{{incremental_interval}}')
    {% endif %}      
    GROUP BY
      tx.hash,
      tx.index,
      tx."from"
  )
SELECT
 'ethereum' as blockchain,
 block_time,
 node_address,
 token_amount
 usd_amount
FROM
  ocr_fulfilled_transactions