{{ config(
        
        alias = 'trades',
        post_hook='{{ expose_spells(\'["ethereum","bnb","polygon","arbitrum","optimism","base"]\',
                                "project",
                                "dodo",
                                \'["scoffie", "owen05"]\') }}'
        )
}}

{% set dodo_models = [
ref('dodo_pools_ethereum_trades')
, ref('dodo_pools_bnb_trades')
, ref('dodo_pools_polygon_trades')
, ref('dodo_pools_arbitrum_trades')
, ref('dodo_pools_optimism_trades')
, ref('dodo_pools_base_trades')
] %}

SELECT *
FROM (
    {% for dex_model in dodo_models %}
    SELECT
        blockchain,
        project,
        version,
        block_date,
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
        tx_hash,
        tx_from,
        tx_to,
        evt_index
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)