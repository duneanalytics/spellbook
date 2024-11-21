{{ config(
    schema = 'balancer_zkevm',
    alias = 'trades'
    )
}}


{% set balancer_models = [
    ref('balancer_v2_zkevm_trades')
] %}

SELECT *
FROM (
    {% for dex_model in balancer_models %}
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
        pool_id,
        swap_fee,
        project_contract_address,
        pool_symbol,
        pool_type,
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
