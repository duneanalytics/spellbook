{{
    config(
        alias='transfers_bpt',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon"]\') }}'
    ) 
}}

{% set transfer_tables = ['balancer_v2_arbitrum.StablePool_evt_Transfer',
                        'balancer_v2_arbitrum.ComposableStablePool_evt_Transfer',
                        'balancer_v2_arbitrum.WeightedPool2Tokens_evt_Transfer',
                        'balancer_v2_arbitrum.WeightedPoolV2_evt_Transfer',
                        'balancer_v2_arbitrum.WeightedPool_evt_Transfer'] %}

SELECT DISTINCT * FROM (
    {% for transfer_table in transfer_tables %}
    SELECT * FROM {{transfer_table}}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}) transfers
