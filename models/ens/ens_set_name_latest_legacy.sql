{{ config(
    schema = 'ens',
    alias = alias('set_name_latest', legacy_model=True),
    tags = ['legacy']
    )
}}

select
      1 as dummy