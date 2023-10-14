{{ config(
    schema = 'sofi_avalanche_c',
    tags = ['legacy', 'static'],
    alias = alias('trades', legacy_model=True)
    )
}}

SELECT 1