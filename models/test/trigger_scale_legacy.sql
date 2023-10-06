{{ config(
        schema='test',
        alias = alias('trigger_scale_legacy', legacy_model=True),
        tags= ['legacy','prod_exclude']
        )
}}

SELECT 1 as test
