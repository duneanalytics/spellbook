{{ config(
    schema='healthcare',
    alias='healthcare_eth_activity_baseline',
    materialized='table'
) }}

-- Daily baseline of Ethereum transaction activity
-- Useful as a reference series to compare against healthcare-related adoption curves later.

select
  date_trunc('day', block_time) as day,
  count(*) as tx_count
from ethereum.transactions
group by 1
order by 1 desc
