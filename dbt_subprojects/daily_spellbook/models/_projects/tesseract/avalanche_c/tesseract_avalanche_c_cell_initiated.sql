{{
    config(
        schema = 'tesseract_avalanche_c',
        alias = 'cell_initiated',
        materialized = 'view'
    )
}}

{{
    tesseract_cell_initiated(
        blockchain = 'avalanche_c'
    )
}}
