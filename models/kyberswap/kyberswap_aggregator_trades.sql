{{ config(
    tags=['dunesql'],

    alias = alias('aggregator_trades'),
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                            "project",
                            "kyberswap",
                            \'["nhd98z"]\') }}'
    )
}}

{% set models = [
ref('kyberswap_aggregator_arbitrum_trades')
] %}


SELECT *
FROM (
    {% for dex_model in models %}
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
--        tx_from,
        tx_to,
        trace_address,
        evt_index
    FROM {{ dex_model }}
    {% if is_incremental() %}
    WHERE block_date >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
