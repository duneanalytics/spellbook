{{ config(
  schema = 'blur_seaport_ethereum',
  alias = alias('base_trades', legacy_model=True),
  tags = ['legacy']
  )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select
  1
