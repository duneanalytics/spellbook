
{% set blockchain = 'arbitrum' %}

{{
    config(
        schema = 'balancer_v2_arbitrum',
        alias = 'bpt_prices',
        materialized = 'table',
        file_format = 'delta',
        post_hook="{{ expose_spells('[\"" + blockchain + '"]' + '\',
                        "project",
                        "balancer_v2",
                        \'["viniabussafi"]\') }}'
    )
}}

{{ 
    balancer_bpt_prices_macro(
        blockchain = blockchain
    )
}}