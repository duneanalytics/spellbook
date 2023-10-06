{{ config(
        schema='test',
        alias = alias('trigger_scale_legacy'),
        tags= ['legacy','prod_exclude']
        )
}}

SELECT 1 as test
