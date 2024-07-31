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
    ref('balancer_arbitrum_trades'),
    ref('balancer_avalanche_c_trades'),
    ref('balancer_base_trades'),
    ref('balancer_ethereum_trades'),
    ref('balancer_gnosis_trades'),
    ref('balancer_optimism_trades'),
    ref('balancer_polygon_trades'),
    ref('balancer_zkevm_trades')
] %}


SELECT *
FROM (
    {% for model in balancer_models %}
        SELECT 
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