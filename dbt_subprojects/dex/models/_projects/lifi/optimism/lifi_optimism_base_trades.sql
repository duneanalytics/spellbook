
{{ config(
        schema = 'lifi_optimism',
        alias = 'base_trades'
        )
}}

{% set lifi_models = [
ref('lifi_v2_optimism_trades')
] %}


SELECT *
FROM (
    {% for dex_model in lifi_models %}
    SELECT
        blockchain,
        project,
        version,
        block_date,
        block_month,
        block_time,
        token_bought_amount_raw,
        token_sold_amount_raw,
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address, --ensure field is explicitly cast as array<bigint> in base models
        evt_index
    FROM {{ dex_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
