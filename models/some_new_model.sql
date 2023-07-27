{{  config(
        schema = '<your-schema>',
        tags = ['dunesql'],
        alias = alias('new_model'),
        materialized = 'table'
    )
}}
select 'test' as test