{{ config(
  schema = 'reservoir',
  alias = alias('tokens_beta', legacy_model=True),
  tags = ['legacy']
  )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select
  1
