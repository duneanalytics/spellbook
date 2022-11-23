{{
    config(
        alias='transfers_bpt',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon"]\') }}'
    ) 
}}

{% set transfer_tables = ['balancer_v2_optimism.ComposableStablePool_evt_Transfer',
                        'balancer_v2_optimism.WeightedPool_evt_Transfer',
                        'balancer_v2_optimism.WeightedPoolV2_evt_Transfer',
                        'balancer_v2_optimism.MetaStablePool_evt_Transfer',
                        'balancer_v2_optimism.WeightedPool2Tokens_evt_Transfer',
                        'balancer_v2_optimism.ReaperLinearPool_evt_Transfer',
                        'balancer_v2_optimism.AaveLinearPool_evt_Transfer'] %}

SELECT DISTINCT * FROM (
    {% for transfer_table in transfer_tables %}
    SELECT * FROM {{transfer_table}}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}) transfers
