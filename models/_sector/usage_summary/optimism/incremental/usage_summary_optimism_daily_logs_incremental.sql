 {{
  config(
        schema = 'usage_summary_optimism',
        alias = 'daily_logs_incremental',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'address', 'block_date'],
        partition_by = ['block_month']
  )
}}

{{usage_summary_daily_logs(
    chain='optimism', start_date='2023-01-01'
)}}