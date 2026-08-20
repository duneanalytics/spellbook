{{ config(
    schema = 'swapwizard',
    alias = 'trades',
    materialized = 'view',
    post_hook='{{ expose_spells(
        blockchains = \'["ethereum", "bnb", "polygon", "base", "arbitrum"]\',
        spell_type = "project",
        spell_name = "swapwizard",
        contributors = \'["cmayorga"]\'
    ) }}'
) }}

{% set swapwizard_models = [
    ref('swapwizard_ethereum_trades'),
    ref('swapwizard_bnb_trades'),
    ref('swapwizard_polygon_trades'),
    ref('swapwizard_base_trades'),
    ref('swapwizard_arbitrum_trades')
] %}

SELECT *
FROM (
    {% for model in swapwizard_models %}
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
