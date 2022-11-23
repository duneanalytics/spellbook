{{
    config(
        alias='transfers_bpt',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon"]\') }}'
    )
}}

{% set transfer_tables = ['balancer_v2_polygon.WeightedPoolV2_evt_Transfer',
                        'balancer_v2_polygon.NoProtocolFeeLiquidityBootstrappingPool_evt_Transfer',
                        'balancer_v2_polygon.WeightedPool_evt_Transfer',
                        'balancer_v2_polygon.LiquidityBootstrappingPool_evt_Transfer',
                        'balancer_v2_polygon.StablePool_evt_Transfer',
                        'balancer_v2_polygon.ComposableStablePool_evt_Transfer',
                        'xavefinance_polygon.xsgd_usdc_v2_evt_Transfer',
                        'balancer_v2_polygon.AaveLinearPool_evt_Transfer'] %}

SELECT DISTINCT * FROM (
    {% for transfer_table in transfer_tables %}
    SELECT * FROM {{transfer_table}}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}) transfers
