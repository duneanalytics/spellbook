{{
    config(
        tags = ['legacy'],
        schema = 'transfers_avalanche_c',
        alias = alias('erc721_agg_hour', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
