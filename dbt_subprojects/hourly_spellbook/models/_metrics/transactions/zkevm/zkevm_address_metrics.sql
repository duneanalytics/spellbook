{{ config(
        schema='zkevm',
        alias = 'address_metrics',
        materialized = 'table',
        tags = ['static'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address']
  )
}}

{{blockchain_address_metrics('zkevm')}}
