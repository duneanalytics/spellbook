{{ config(
        schema='blast',
        alias = 'address_metrics',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address'],
        tags=['static'],
        post_hook='{{ hide_spells() }}'
  )
}}

{{blockchain_address_metrics('blast')}}
