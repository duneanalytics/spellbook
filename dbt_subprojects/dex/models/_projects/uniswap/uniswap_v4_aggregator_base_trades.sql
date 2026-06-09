{{ config(
        schema = 'uniswap_v4',
        alias = 'aggregator_base_trades'
        )
}}

SELECT *
FROM (
    {% for chain in uniswap_v4_chains() %}
    SELECT
        blockchain,
        project,
        version,
        block_month,
        block_date,
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
    FROM {{ ref('uniswap_v4_' ~ chain ~ '_aggregator_base_trades') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
