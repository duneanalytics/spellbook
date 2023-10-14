{{ config(
    schema = 'sofi_bnb',
    tags = ['legacy', 'static'],
    alias = alias('trades', legacy_model=True)
    )
}}

SELECT 1