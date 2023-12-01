 {{
  config(
        schema = 'contracts_optimism',
        alias = 'daily_usage_summary',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['blockchain', 'contract_address', 'block_date'],
        partition_by = ['block_month']
  )
}}

{{contracts_daily_usage_summary(
    chain='optimism'
)}}