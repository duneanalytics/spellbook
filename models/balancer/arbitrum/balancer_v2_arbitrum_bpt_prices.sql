
{% set blockchain = 'arbitrum' %}

{{
    config(
        schema='balancer_v2_' + blockchain,
        alias = 'bpt_prices',
        materialized = 'table',
        file_format = 'delta',
        post_hook="{{ expose_spells('[\"" + blockchain + '"]' + '\',
                        "project",
                        "balancer_v2",
                        \'["stefenon", "viniabussafi"]\') }}'
    )
}}

{{ 
    balancer_bpt_prices_macro(
        blockchain = blockchain
    )
}}