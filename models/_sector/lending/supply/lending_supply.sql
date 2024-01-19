{{
  config(
    schema = 'lending',
    alias = 'supply',
    partition_by = ['blockchain', 'project', 'block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'transaction_type', 'token_address', 'tx_hash', 'evt_index'],
  )
}}

{{
  lending_enrich_supply(
    model = ref('lending_base_supply')
  )
}}
