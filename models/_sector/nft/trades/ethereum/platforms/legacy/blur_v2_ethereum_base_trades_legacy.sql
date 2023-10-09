{{ config(
    schema = 'blur_v2_ethereum',
    alias = alias('base_trades', legacy_model=True),
    tags = ['legacy','remove']
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select
  1
