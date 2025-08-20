{{
    config(
        schema = 'tesseract_avalanche_c',
        alias = 'ictt_pairs',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['token_home_blockchain_id', 'token_home_address', 'token_remote_blockchain_id', 'token_remote_address']
    )
}}

{{
    tesseract_ictt_pairs(
        blockchain = 'avalanche_c'
    )
}}
