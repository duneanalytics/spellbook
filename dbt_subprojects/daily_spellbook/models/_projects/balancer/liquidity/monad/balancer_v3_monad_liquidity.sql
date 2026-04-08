
{% set blockchain = 'monad' %}

{{
    config(
        schema = 'balancer_v3_monad',
        alias = 'liquidity',
        materialized = 'table',
        file_format = 'delta',
        post_hook = '{{ expose_spells(\'["monad"]\',
                                    "project",
                                    "balancer_v3_monad",
                                    \'["balancerlabs"]\') }}'
    )
}}


{{ 
    balancer_v3_compatible_liquidity_macro(
        blockchain = blockchain,
        version = '3',        
        project_decoded_as = 'balancer_v3',
        base_spells_namespace = 'balancer',
        pool_labels_model = 'balancer_v3_pools_monad'
    )
}}
