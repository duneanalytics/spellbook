{{ config(
    schema = 'sofi_base',
    tags = ['legacy', 'static'],
    alias = alias('trades', legacy_model=True),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index']
    )
}}

SELECT 1