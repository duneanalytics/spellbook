{{ config(
  schema = 'sharky_solana',
  alias = alias('events', legacy_model=True),
  tags = ['legacy']
  )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select
  1