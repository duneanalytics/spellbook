{{ config(
        alias = alias('trades'),
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "polygon", "bnb"]\',
                                "project",
                                "xchange",
                                \'["mike-x7f"]\') }}'
        )
}}

{% set xchange_models = [
ref('xchange_ethereum_trades')
, ref('xchange_arbitrum_trades')
, ref('xchange_polygon_trades')
, ref('xchange_bnb_trades')
] %}


SELECT *
FROM (
    {% for dex_model in xchange_models %}
    SELECT
        blockchain,
        project,
        version,
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
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;