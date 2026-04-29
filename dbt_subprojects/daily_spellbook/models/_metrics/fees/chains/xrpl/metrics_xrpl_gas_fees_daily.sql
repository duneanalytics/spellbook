{{
  config(
    schema = 'metrics_xrpl',
    alias = 'gas_fees_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
  )
}}

-- Temporary placeholder while XRPL gas outputs stay in the tokens rollout path.
select
  cast('xrpl' as varchar) as blockchain,
  cast(null as date) as block_date,
  cast(null as double) as gas_fees_usd
where false