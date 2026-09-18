{{
  config(
    schema = 'addresses_arc',
    alias = 'stats',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'address'],
    filtering_columns = []
  )
}}

{{ addresses_stats('arc', funding_relation=ref('addresses_events_arc_first_funded_by')) }}
