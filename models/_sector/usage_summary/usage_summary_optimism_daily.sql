 {{
  config(
        schema = 'usage_summary_optimism',
        alias = 'daily',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'address', 'block_date'],
        partition_by = ['block_month']
  )
}}

{{contracts_daily_usage_summary(
    chain='optimism'
)}}