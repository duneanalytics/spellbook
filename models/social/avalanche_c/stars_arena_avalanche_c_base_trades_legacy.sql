{{ config(
    schema = 'stars_arena_avalanche_c',
    tags = ['legacy', 'static'],
    alias = alias('base_trades', legacy_model=True),
    )
}}

SELECT 1