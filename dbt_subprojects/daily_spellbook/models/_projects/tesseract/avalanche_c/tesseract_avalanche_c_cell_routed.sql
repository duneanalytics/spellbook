{{
    config(
        schema = 'tesseract_avalanche_c',
        alias = 'cell_routed',
        materialized = 'view'
    )
}}

{{
    tesseract_cell_routed(
        blockchain = 'avalanche_c'
    )
}}
