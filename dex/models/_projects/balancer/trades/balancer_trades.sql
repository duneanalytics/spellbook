{{ config(
        schema = 'balancer',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                                      spell_type = "project", 
                                      spell_name = "balancer", 
                                      contributors = \'["bizzyvinci", "thetroyharris", "viniabussafi"]\') }}'
        )
}}

{% set balancer_models = [
    ref('balancer_v2_arbitrum_pools_fees'),
    ref('balancer_v2_avalanche_c_pools_fees'),
    ref('balancer_v2_base_pools_fees'),
    ref('balancer_v2_ethereum_pools_fees'),
    ref('balancer_v2_gnosis_pools_fees'),
    ref('balancer_v2_optimism_pools_fees'),
    ref('balancer_v2_polygon_pools_fees'),
    ref('balancer_v2_zkevm_pools_fees')
] %}


SELECT *
FROM (
    {% for model in balancer_models %}
    blockchain,
    project,
    version,
    block_date,
    block_number,
    block_month,
    block_time,
    token_bought_symbol,
    token_sold_symbol,
    token_pair,
    token_bought_amount,
    token_sold_amount,
    token_bought_amount_raw,
    token_sold_amount_raw,
    amount_usd,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    pool_id,
    swap_fee,
    pool_symbol,
    pool_type,
    tx_hash,
    tx_from,
    tx_to,
    evt_index
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)