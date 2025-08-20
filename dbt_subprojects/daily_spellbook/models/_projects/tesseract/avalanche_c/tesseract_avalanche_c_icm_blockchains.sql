{{
    config(
        schema = 'tesseract_avalanche_c',
        alias = 'icm_blockchains',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain_id']
    )
}}

{{
    tesseract_icm_blockchains(
        blockchain = 'avalanche_c'
    )
}}
