{{ config(
	tags=['legacy'],

    alias = alias('userops', legacy_model=True)
)}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select
  1