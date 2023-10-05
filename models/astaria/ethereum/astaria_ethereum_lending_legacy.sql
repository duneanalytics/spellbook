{{ config(
    schema = 'astaria_ethereum',
    alias = alias('lending', legacy_model=True),
    tags = ['legacy']
    )
}}

SELECT 

    1 as dummy  