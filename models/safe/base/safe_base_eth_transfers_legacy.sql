{{ config(
  schema = 'safe_base',
  alias = alias('eth_transfers', legacy_model=True),
  tags = ['legacy']
  )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select
  1 as dummy