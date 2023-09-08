{{
    config(
        tags = ['legacy'],
        schema = 'transfers_fantom',
        alias = alias('erc1155', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
