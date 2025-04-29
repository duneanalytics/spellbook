{{ config(
        schema='sei',
        alias = 'address_metrics',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address']
  )
}}

{{blockchain_address_metrics('sei')}}
