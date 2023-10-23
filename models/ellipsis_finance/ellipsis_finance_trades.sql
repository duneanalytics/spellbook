{{ config(
        
        alias = 'trades',
        partition_by = ['block_month'],
        post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "ellipsis_finance",
                                \'["Henrystats"]\') }}'
        )
}}

{% set ellipsis_finance_models = [
ref('ellipsis_finance_bnb_trades')
] %}


SELECT *
FROM (
    {% for dex_model in ellipsis_finance_models %}
    SELECT
        blockchain,
        project,
        version,
        block_date,
        block_time,
        block_month,
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