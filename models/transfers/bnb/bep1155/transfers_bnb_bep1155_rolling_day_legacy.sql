{{
    config(
        tags = ['legacy'],
        schema = 'transfers_bnb',
        alias = alias('bep1155_rolling_day', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
