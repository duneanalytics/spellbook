 {{
  config(
        schema = 'usage_summary_optimism',
        alias = 'daily_transactions_2021',
        materialized ='table',
        unique_key = ['blockchain', 'address', 'block_date'],
        partition_by = ['block_month']
  )
}}

{{usage_summary_daily_transactions(
    chain='optimism', start_date='2021-01-01', end_date='2022-01-01'
)}}

UNION ALL

{{usage_summary_daily_transactions(
    chain='optimism_legacy', start_date='2021-01-01', end_date='2022-01-01'
)}}