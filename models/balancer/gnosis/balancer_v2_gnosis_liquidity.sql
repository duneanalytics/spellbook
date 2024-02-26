
{% set blockchain = 'gnosis' %}

{{
    config(
    schema = 'balancer_v2_gnosis',
        alias = 'liquidity',
        materialized = 'table',
        file_format = 'delta',
        post_hook="{{ expose_spells('[\"" + blockchain + '"]' + '\',
                        "project",
                        "balancer_v2",
                        \'["stefenon", "viniabussafi"]\') }}'
    )
}}

{{ 
    balancer_liquidity_macro(
        blockchain = blockchain,
        version = '2'
    )
}}
