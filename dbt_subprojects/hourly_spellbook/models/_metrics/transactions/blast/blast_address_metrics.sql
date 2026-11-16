{{ config(
        schema='blast',
        alias = 'address_metrics',
        materialized = 'table',
        file_format = 'delta',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
  )
}}

{{blockchain_address_metrics('blast')}}
