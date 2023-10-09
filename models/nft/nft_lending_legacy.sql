{{ config(
    schema = 'nft',
    alias = alias('lending', legacy_model=True),
    tags = ['legacy','remove']
    )
}}

SELECT
    1
