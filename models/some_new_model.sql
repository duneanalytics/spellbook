{{  config(
        schema = 'test_new_dunesql',
        tags = ['dunesql'],
        alias = alias('new_model'),
        materialized = 'table'
    )
}}
select 'test' as test