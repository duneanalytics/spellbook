{{ config(
    schema = 'bitget_dex_aggregator',
    alias = 'trades',
    materialized = 'view',
    post_hook='{{ expose_spells(
        blockchains = \'["bnb", "ethereum", "arbitrum", "polygon", "base"]\',
        spell_type = "project",
        spell_name = "bitget_dex_aggregator",
        contributors = \'["kunwh"]\'
    ) }}'
) }}

{% set bitget_dex_aggregator_models = [
    ref('bitget_dex_aggregator_bnb_trades'),
    ref('bitget_dex_aggregator_ethereum_trades'),
    ref('bitget_dex_aggregator_arbitrum_trades'),
    ref('bitget_dex_aggregator_polygon_trades'),
    ref('bitget_dex_aggregator_base_trades')
] %}

SELECT *
FROM (
    {% for model in bitget_dex_aggregator_models %}
    SELECT
        blockchain,
        project,
        version,
        block_month,
        block_date,
        block_time,
        block_number,
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
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
