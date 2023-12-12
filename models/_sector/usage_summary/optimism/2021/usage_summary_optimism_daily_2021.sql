 {{
  config(
        schema = 'usage_summary_optimism',
        alias = 'daily_2021',
        materialized ='table',
        unique_key = ['blockchain', 'address', 'block_date'],
        partition_by = ['block_month']
  )
}}

{{usage_summary_daily(
    chain='optimism', date_string='2021'
)}}