{{
    config(
        tags = ['legacy', 'remove'],
        schema = 'nft_polygon',
        alias = alias('native_mints', legacy_model=True)
    )
}}

select 1
