{{ config(
	tags=['legacy'],
  schema = 'mauve',
  alias = alias('trades', legacy_model=True),
  )
}}

SELECT
  1