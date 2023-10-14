{{ config(
    schema = 'post_tech_arbitrum',
    tags = ['legacy', 'static'],
    alias = alias('base_trades', legacy_model=True),
    )
}}

SELECT 1