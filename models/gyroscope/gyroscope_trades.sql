{{ config(
    schema = 'gyroscope',
    alias = 'trades',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["arbitrum", "ethereum","optimism", "polygon"]\',
                                "project",
                                "gyroscope",
                                \'["fmarrr"]\') }}'
    )
}}

{% set gyroscope_models = [
    ref('gyroscope_arbitrum_trades'),
    ref('gyroscope_ethereum_trades'),
    ref('gyroscope_optimism_trades'),
    ref('gyroscope_polygon_trades')
] %}


SELECT *
FROM (
    {% for dex_model in gyroscope_models %}
    SELECT
        blockchain,
        project,
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
        pool_id,
        swap_fee,
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
