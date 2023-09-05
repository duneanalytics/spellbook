{{ config(
  schema = 'safe_base',
  alias = alias('safes', legacy_model=True),
  tags = ['legacy']
  )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select
  1 as dummy