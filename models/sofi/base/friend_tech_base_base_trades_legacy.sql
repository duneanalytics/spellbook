{{ config(
    schema = 'friend_tech_base',
    tags = ['legacy', 'static'],
    alias = alias('base_trades', legacy_model=True),
    )
}}

SELECT 1