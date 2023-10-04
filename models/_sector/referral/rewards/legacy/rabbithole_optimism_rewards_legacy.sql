{{ config(
    schema = 'rabbithole_optimism',
    alias = alias('rewards', legacy_model=True),
    tags = ['legacy']
    )
}}


-- DUMMY TABLE, WILL BE REMOVED SOON
select
  1
