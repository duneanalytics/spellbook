{{
    config(
        tags = ['legacy'],
        schema = 'transfers_fantom',
        alias = alias('erc1155_agg_day', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
