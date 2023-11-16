{{
    config(
        schema = 'moola_celo',
        alias = 'base_borrow',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['transaction_type', 'token_address', 'evt_tx_hash', 'evt_index']
    )
}}

{{
  lending_aave_v2_compatible_borrow(
    blockchain = 'celo',
    project = 'moola',
    version = '1',
    project_decoded_as = 'moolainterestbearingmoo'
  )
}}
