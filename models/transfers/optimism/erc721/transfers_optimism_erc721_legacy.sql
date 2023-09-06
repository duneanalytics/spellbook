{{
    config(
        tags = ['legacy'],
        schema = 'transfers_optimism',
        alias = alias('erc721', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
