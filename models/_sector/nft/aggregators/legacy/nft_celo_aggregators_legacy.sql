{{
    config(
        tags = ['legacy'],
        schema = 'nft_celo',
        alias = alias('aggregators', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
