{{ config(
        tags = ['dunesql'],
        alias = alias('aggregator_trades'),
        post_hook='{{ expose_spells(\'["ethereum","bnb","polygon","arbitrum","optimism"]\',
                                "project",
                                "dodo",
                                \'["owen05"]\') }}'
        )
}}

{% set dodo_models = [
ref('dodo_aggregator_ethereum_trades')
, ref('dodo_aggregator_bnb_trades')
, ref('dodo_aggregator_polygon_trades')
, ref('dodo_aggregator_arbitrum_trades')
, ref('dodo_aggregator_optimism_trades')
] %}


SELECT *
FROM (
    {% for dex_model in dodo_models %}
    SELECT
        blockchain,
        project,
        version,
        block_month,
        block_date,
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
        trace_address,
        evt_index
    FROM {{ dex_model }}
    {% if is_incremental() %}
    WHERE block_date >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)