{{ config(
    schema = 'tokens_gnosis',
    alias = 'suicide_events',
    materialized = 'table',
    file_format = 'delta',
    )
}}

-- Ordered SUICIDE (SELFDESTRUCT) trace events on Gnosis.
-- Staging table shared by tokens_gnosis_suicide_transfers so that each hourly
-- build reads this small table (~117K rows total) instead of scanning the full
-- gnosis.traces partition three times.
--
-- The ORDER BY includes (block_number, tx_index, trace_address) as a tiebreaker
-- after block_time so per-address event_sequence is deterministic even when the
-- same address is self-destructed multiple times inside the same block.

SELECT
     cast(date_trunc('month', block_time) as date) AS block_month
    , cast(date_trunc('day', block_time) as date) AS block_date
    , block_time
    , block_number
    , tx_hash
    , tx_index
    , trace_address
    , tx_from
    , tx_to
    , address
    , refund_address
    , ROW_NUMBER() OVER (
          PARTITION BY address
          ORDER BY block_time, block_number, tx_index, array_join(trace_address, ',')
      ) AS event_sequence
    , LAG(block_time) OVER (
          PARTITION BY address
          ORDER BY block_time, block_number, tx_index, array_join(trace_address, ',')
      ) AS previous_block_time
FROM
    {{ source('gnosis', 'traces') }}
WHERE
    type = 'suicide'
    AND
    success
