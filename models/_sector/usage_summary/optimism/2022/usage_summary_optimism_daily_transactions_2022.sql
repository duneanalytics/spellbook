 {{
  config(
        schema = 'usage_summary_optimism',
        alias = 'daily_transactions_2022',
        materialized ='table',
        unique_key = ['blockchain', 'address', 'block_date'],
        partition_by = ['block_month']
  )
}}

{{usage_summary_daily_transactions(
    chain='optimism', start_date='2022-01-01', end_date='2022-01-01'
)}}