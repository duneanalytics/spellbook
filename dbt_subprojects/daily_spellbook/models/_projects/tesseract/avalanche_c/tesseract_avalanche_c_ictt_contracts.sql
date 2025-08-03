{{
    config(
        schema = 'tesseract_avalanche_c',
        alias = 'ictt_contracts',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['contract_address']
    )
}}

{{
    tesseract_ictt_contracts(
        blockchain = 'avalanche_c'
    )
}}
