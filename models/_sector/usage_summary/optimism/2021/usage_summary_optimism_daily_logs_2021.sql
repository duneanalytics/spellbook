 {{
  config(
        schema = 'usage_summary_optimism',
        alias = 'daily_logs_2021',
        materialized ='table',
        unique_key = ['blockchain', 'address', 'block_date'],
        partition_by = ['block_month']
  )
}}

{{usage_summary_daily_logs(
    chain='optimism', '2021-01-01', '2022-01-01'
)}}

UNION ALL

{{usage_summary_daily_logs(
    chain='optimism_legacy', '2021-01-01', '2022-01-01'
)}}