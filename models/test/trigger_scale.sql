{{ config(
        schema='test',
        alias = alias('trigger_scale'),
        tags= ['dunesql','prod_exclude']
        )
}}

/*
this dummy model is intended only to be used in the gh action for PR CI tests
the goal is to run this on loop until the cluster is spun up, as a model run helps turn the cluster back on after auto-shutdown
*/

SELECT 1 as test
