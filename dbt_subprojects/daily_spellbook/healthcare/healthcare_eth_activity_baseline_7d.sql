Rights Reserved, Unlicensed
{{ config(
    schema='healthcare',
    alias='healthcare_eth_activity_baseline_7d',
    materialized='table'
) }}

with base as (
  select
    date_trunc('day', block_time) as day,
    count(*) as tx_count
  from ethereum.transactions
  group by 1
),
series as (
  select
    day,
    tx_count,
    avg(tx_count) over (
      order by day
      rows between 6 preceding and current row
    ) as tx_count_ma7
  from base
)
select * from series
order by day desc
