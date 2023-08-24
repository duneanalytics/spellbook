{{ config(
    schema = 'astaria_ethereum',
    alias = alias('daily_deposits', legacy_model=True),
    tags = ['legacy']
    )
}}

SELECT 

    1 as dummy