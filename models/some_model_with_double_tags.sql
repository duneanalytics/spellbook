{{ config(
    schema = 'test',
    alias = alias('double_tags'),
    tags = ['dunesql', 'legacy']
)}}
select 'test'
