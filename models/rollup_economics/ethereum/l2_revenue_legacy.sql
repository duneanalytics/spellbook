{{ config(
  schema = 'rollup_economics_ethereum',
  alias = alias('l2_revenue', legacy_model=True),
  tags = ['legacy']
  )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select
    1
