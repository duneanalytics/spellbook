{{
  config(
    schema = 'lending',
    alias = 'borrow',
    partition_by = ['blockchain', 'project', 'block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'transaction_type', 'token_address', 'evt_tx_hash', 'evt_index'],
    post_hook = '{{ expose_spells(\'["lending"]\',
                                    "sector",
                                    "borrow",
                                    \'["tomfutago"]\') }}'
  )
}}

{{
  lending_enrich_borrow(
    model = ref('_borrow_stg')
  )
}}
