 {{
  config(
        schema = 'usage_summary_optimism',
        alias = 'daily_logs_2022',
        materialized ='table',
        unique_key = ['blockchain', 'address', 'block_date'],
        partition_by = ['block_month']
  )
}}

{{usage_summary_daily_logs(
    chain='optimism', '2022-01-01', '2022-01-01'
)}}