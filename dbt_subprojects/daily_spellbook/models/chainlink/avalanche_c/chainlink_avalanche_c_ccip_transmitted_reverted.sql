{{
  config(

    alias='ccip_transmitted_reverted',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'node_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}



-- CUR2-2973: bound the previously-unbounded side of the logs<->transactions join by its own
-- block_time so the incremental run prunes it via Delta file-skipping. The added predicate is
-- logically redundant -- the joined log and transaction are in the same block, so their
-- block_time is identical, and the other side is already bounded to the incremental window --
-- so results are unchanged (proven EXCEPT=0); it only stops the full-history scan. Incremental-only.

WITH
  avalanche_c_usd AS (
    SELECT
      minute as block_time,
      price as usd_amount
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      symbol = 'AVAX'
      {% if is_incremental() %}
        AND {{ incremental_predicate('minute') }}
      {% endif %}
  ),
  ccip_reverted_transactions AS (
    SELECT
      tx.tx_hash as tx_hash,
      tx.index as tx_index,
      MAX(tx.block_time) as block_time,
      cast(date_trunc('month', MAX(tx.block_time)) as date) as date_month,
      tx.tx_from as "node_address",
      MAX((cast((gas_used) as double) / 1e18) * gas_price) as token_amount,
      MAX(avalanche_c_usd.usd_amount) as usd_amount
    FROM
      {{ ref('chainlink_avalanche_c_ccip_transmitted_logs') }} tx
      LEFT JOIN {{ source('avalanche_c', 'transactions') }} tx2 ON tx2.hash = tx.tx_hash
      {% if is_incremental() %}
        AND {{ incremental_predicate('tx.block_time') }}
      {% endif %}
      LEFT JOIN avalanche_c_usd ON date_trunc('minute', tx.block_time) = avalanche_c_usd.block_time
    WHERE
      tx2.success = false
      {% if is_incremental() %}
        AND {{ incremental_predicate('tx.block_time') }}
        AND {{ incremental_predicate('tx2.block_time') }}
      {% endif %}
    GROUP BY
      tx.tx_hash,
      tx.index,
      tx.tx_from
  )
SELECT
 'avalanche_c' as blockchain,
  block_time,
  date_month,
  node_address,
  token_amount,
  usd_amount,
  tx_hash,
  tx_index
FROM
  ccip_reverted_transactions