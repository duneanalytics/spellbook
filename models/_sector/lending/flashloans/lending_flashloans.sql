{{
  config(
    schema = 'lending',
    alias = 'flashloans',
    partition_by = ['blockchain', 'project', 'block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
  )
}}

{{
  lending_enrich_flashloans(
    model = ref('lending_base_flashloans')
  )
}}
