{{
  config(
    schema = 'lending',
    alias = 'borrow',
    partition_by = ['blockchain', 'project', 'block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'transaction_type', 'token_address', 'tx_hash', 'evt_index'],
  )
}}

{{
  lending_enrich_borrow(
    model = ref('lending_base_borrow')
  )
}}
