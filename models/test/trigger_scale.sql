{{ config(
        schema='test',
        alias = alias('trigger_scale'),
        tags= ['dunesql','prod_exclude']
        )
}}

SELECT 1 as test
