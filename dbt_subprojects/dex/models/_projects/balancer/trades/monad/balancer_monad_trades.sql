{{ config(
    schema = 'balancer_monad',
    alias = 'trades'
    )
}}


{% set balancer_models = [
    ref('balancer_v3_monad_trades')
] %}

SELECT *
FROM (
    {% for dex_model in balancer_models %}
    SELECT
        t.blockchain,
        t.project,
        t.version,
        t.block_month,
        t.block_date,
        t.block_time,
        t.block_number,
        t.token_bought_symbol,
        t.token_sold_symbol,
        t.token_pair,
        t.token_bought_amount,
        t.token_sold_amount,
        t.token_bought_amount_raw,
        t.token_sold_amount_raw,
        t.amount_usd,
        t.token_bought_address,
        t.token_sold_address,
        t.taker,
        t.maker,
        t.pool_id,
        t.swap_fee,
        t.project_contract_address,
        t.pool_symbol,
        t.pool_type,
        t.tx_hash,
        t.tx_from,
        t.tx_to,
        t.evt_index
    FROM {{ dex_model }} AS t
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

