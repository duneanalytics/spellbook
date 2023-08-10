{{ config(
    schema = 'astaria_ethereum',
    alias = alias('events', legacy_model=True),
    tags = ['legacy']
    )
}}

SELECT 

    1 as dummy 