{{
    config(
        alias='transfers_bpt',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon"]\') }}'
    )
}}

{% set transfer_tables = ['balancer_v2_ethereum.StablePhantomPool_evt_Transfer',
                        'balancer_v2_ethereum.WeightedPoolV2_evt_Transfer',
                        'element_finance_ethereum.ConvergentCurvePool_evt_Transfer',
                        'balancer_v2_ethereum.ComposableStablePool_evt_Transfer',
                        'balancer_v2_ethereum.LiquidityBootstrappingPool_evt_Transfer',
                        'balancer_v2_ethereum.WeightedPool_evt_Transfer',
                        'element_ethereum.ConvergentCurvePool_evt_Transfer',
                        'balancer_v2_ethereum.NoProtocolFeeLiquidityBootstrappingPool_evt_Transfer',
                        'balancer_v2_ethereum.InvestmentPool_evt_Transfer',
                        'balancer_v2_ethereum.StablePool_evt_Transfer',
                        'balancer_v2_ethereum.AaveLinearPool_evt_Transfer',
                        'sensefinance_ethereum.Space_evt_Transfer',
                        'balancer_v2_ethereum.MetaStablePool_evt_Transfer',
                        'aura_finance_ethereum.StablePool_evt_Transfer',
                        'balancer_v2_ethereum.ConvergentCurvePool_evt_Transfer'] %}

SELECT DISTINCT * FROM (
    {% for transfer_table in transfer_tables %}
    SELECT * FROM {{transfer_table}}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}) transfers
