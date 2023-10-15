{{
    config(
        tags = ['legacy', 'remove'],
        schema = 'nft_celo',
        alias = alias('native_mints', legacy_model=True)
    )
}}

select 1
