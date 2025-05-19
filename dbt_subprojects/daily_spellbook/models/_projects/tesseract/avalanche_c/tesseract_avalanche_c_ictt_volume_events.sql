{% set blockchain = 'avalanche_c' %}

{{
    config(
        schema = 'tesseract_' + blockchain,
        alias = 'ictt_volume_events',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['evt_block_number', 'evt_index']
    )
}}

{{
    tesseract_ictt_volume_events(
        blockchain = blockchain
    )
}}
