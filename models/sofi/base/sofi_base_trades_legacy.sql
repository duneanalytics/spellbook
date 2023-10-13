{{ config(
    schema = 'sofi_base',
    tags = ['legacy', 'static'],
    alias = alias('trades', legacy_model=True)
    )
}}

SELECT 1