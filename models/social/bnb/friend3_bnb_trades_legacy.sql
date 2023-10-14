{{ config(
    schema = 'friend3_bnb',
    tags = ['legacy', 'static'],
    alias = alias('base_trades', legacy_model=True),
    )
}}

SELECT 1