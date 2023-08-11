{{ config(
    schema = 'rubicon_arbitrum',
	tags=['legacy'],
    alias = alias('trades', legacy_model=True)
)
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1 as dummy